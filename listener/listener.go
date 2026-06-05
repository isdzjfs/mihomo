package listener

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/mihomo/common/atomic"
	C "github.com/metacubex/mihomo/constant"
	LC "github.com/metacubex/mihomo/listener/config"
	"github.com/metacubex/mihomo/listener/http"
	"github.com/metacubex/mihomo/listener/mixed"
	"github.com/metacubex/mihomo/listener/redir"
	embedSS "github.com/metacubex/mihomo/listener/shadowsocks"
	"github.com/metacubex/mihomo/listener/sing_shadowsocks"
	"github.com/metacubex/mihomo/listener/sing_tun"
	"github.com/metacubex/mihomo/listener/sing_vmess"
	"github.com/metacubex/mihomo/listener/socks"
	"github.com/metacubex/mihomo/listener/tproxy"
	"github.com/metacubex/mihomo/listener/tuic"
	LT "github.com/metacubex/mihomo/listener/tunnel"
	"github.com/metacubex/mihomo/log"

	"github.com/samber/lo"
)

var (
	bindState = atomic.NewTypedValue(listenerBindState{
		allowLan:    false,
		bindAddress: "*",
	})

	socksListener       *socks.Listener
	socksUDPListener    *socks.UDPListener
	httpListener        *http.Listener
	redirListener       *redir.Listener
	redirUDPListener    *tproxy.UDPListener
	tproxyListener      *tproxy.Listener
	tproxyUDPListener   *tproxy.UDPListener
	mixedListener       *mixed.Listener
	mixedUDPLister      *socks.UDPListener
	tunnelTCPListeners  = map[string]*LT.Listener{}
	tunnelUDPListeners  = map[string]*LT.PacketConn{}
	inboundListeners    = map[string]C.InboundListener{}
	tunLister           *sing_tun.Listener
	shadowSocksListener C.MultiAddrListener
	vmessListener       *sing_vmess.Listener
	tuicListener        *tuic.Listener

	// lock for recreate function
	socksMux   sync.Mutex
	httpMux    sync.Mutex
	redirMux   sync.Mutex
	tproxyMux  sync.Mutex
	mixedMux   sync.Mutex
	tunnelMux  sync.Mutex
	inboundMux sync.Mutex
	tunMux     sync.Mutex
	ssMux      sync.Mutex
	vmessMux   sync.Mutex
	tuicMux    sync.Mutex

	LastTunConf  LC.Tun
	LastTuicConf LC.TuicServer
)

type listenerBindState struct {
	allowLan    bool
	bindAddress string
}

type Ports struct {
	Port              int    `json:"port"`
	SocksPort         int    `json:"socks-port"`
	RedirPort         int    `json:"redir-port"`
	TProxyPort        int    `json:"tproxy-port"`
	MixedPort         int    `json:"mixed-port"`
	ShadowSocksConfig string `json:"ss-config"`
	VmessConfig       string `json:"vmess-config"`
}

func GetTunConf() LC.Tun {
	if tunLister == nil {
		return LastTunConf
	}
	return tunLister.Config()
}

func GetTuicConf() LC.TuicServer {
	if tuicListener == nil {
		return LC.TuicServer{Enable: false}
	}
	return tuicListener.Config()
}

func AllowLan() bool {
	return bindState.Load().allowLan
}

func BindAddress() string {
	return bindState.Load().bindAddress
}

func SetAllowLan(al bool) {
	updateBindState(func(next *listenerBindState) {
		next.allowLan = al
	})
}

func SetBindAddress(host string) {
	updateBindState(func(next *listenerBindState) {
		next.bindAddress = host
	})
}

func updateBindState(update func(*listenerBindState)) {
	for {
		current := bindState.Load()
		next := current
		update(&next)
		if bindState.CompareAndSwap(current, next) {
			return
		}
	}
}

func genListenAddr(port int) string {
	state := bindState.Load()
	return genAddr(state.bindAddress, port, state.allowLan)
}

func shouldCloseBeforeListen(oldAddr string, newAddr string) bool {
	if oldAddr == newAddr {
		return false
	}
	oldHost, oldPort, oldErr := net.SplitHostPort(oldAddr)
	newHost, newPort, newErr := net.SplitHostPort(newAddr)
	if oldErr != nil || newErr != nil || oldPort != newPort {
		return false
	}
	return oldHost == newHost || isWildcardListenHost(oldHost) || isWildcardListenHost(newHost)
}

func isWildcardListenHost(host string) bool {
	switch strings.TrimSpace(host) {
	case "", "*", "0.0.0.0", "::":
		return true
	default:
		return false
	}
}

func ReCreateHTTP(port int, tunnel C.Tunnel) error {
	httpMux.Lock()
	defer httpMux.Unlock()

	var err error
	defer func() {
		if err != nil {
			log.Errorln("Start HTTP server error: %s", err.Error())
		}
	}()

	addr := genListenAddr(port)

	oldListener := httpListener
	if oldListener != nil && oldListener.RawAddress() == addr {
		return nil
	}

	if portIsZero(addr) {
		if oldListener != nil {
			_ = oldListener.Close()
			httpListener = nil
		}
		return nil
	}

	if oldListener != nil && shouldCloseBeforeListen(oldListener.RawAddress(), addr) {
		_ = oldListener.Close()
		httpListener = nil
		oldListener = nil
	}

	newListener, err := http.New(addr, tunnel)
	if err != nil {
		return err
	}
	httpListener = newListener
	if oldListener != nil {
		_ = oldListener.Close()
	}

	log.Infoln("HTTP proxy listening at: %s", httpListener.Address())
	return nil
}

func ReCreateSocks(port int, tunnel C.Tunnel) error {
	socksMux.Lock()
	defer socksMux.Unlock()

	var err error
	defer func() {
		if err != nil {
			log.Errorln("Start SOCKS server error: %s", err.Error())
		}
	}()

	addr := genListenAddr(port)

	shouldTCPIgnore := false
	shouldUDPIgnore := false
	oldTCPListener := socksListener
	oldUDPListener := socksUDPListener

	if oldTCPListener != nil {
		if oldTCPListener.RawAddress() == addr {
			shouldTCPIgnore = true
		}
	}

	if oldUDPListener != nil {
		if oldUDPListener.RawAddress() == addr {
			shouldUDPIgnore = true
		}
	}

	if shouldTCPIgnore && shouldUDPIgnore {
		return nil
	}

	if portIsZero(addr) {
		if oldTCPListener != nil {
			_ = oldTCPListener.Close()
			socksListener = nil
		}
		if oldUDPListener != nil {
			_ = oldUDPListener.Close()
			socksUDPListener = nil
		}
		return nil
	}

	if !shouldTCPIgnore && oldTCPListener != nil && shouldCloseBeforeListen(oldTCPListener.RawAddress(), addr) {
		_ = oldTCPListener.Close()
		socksListener = nil
		oldTCPListener = nil
	}
	if !shouldUDPIgnore && oldUDPListener != nil && shouldCloseBeforeListen(oldUDPListener.RawAddress(), addr) {
		_ = oldUDPListener.Close()
		socksUDPListener = nil
		oldUDPListener = nil
	}

	tcpListener := oldTCPListener
	if !shouldTCPIgnore {
		tcpListener, err = socks.New(addr, tunnel)
		if err != nil {
			return err
		}
	}

	udpListener := oldUDPListener
	if !shouldUDPIgnore {
		udpListener, err = socks.NewUDP(addr, tunnel)
		if err != nil {
			if !shouldTCPIgnore {
				_ = tcpListener.Close()
			}
			return err
		}
	}

	socksListener = tcpListener
	socksUDPListener = udpListener
	if !shouldTCPIgnore && oldTCPListener != nil {
		_ = oldTCPListener.Close()
	}
	if !shouldUDPIgnore && oldUDPListener != nil {
		_ = oldUDPListener.Close()
	}

	log.Infoln("SOCKS proxy listening at: %s", socksListener.Address())
	return nil
}

func ReCreateRedir(port int, tunnel C.Tunnel) error {
	redirMux.Lock()
	defer redirMux.Unlock()

	var err error
	defer func() {
		if err != nil {
			log.Errorln("Start Redir server error: %s", err.Error())
		}
	}()

	addr := genListenAddr(port)

	oldTCPListener := redirListener
	oldUDPListener := redirUDPListener
	shouldTCPIgnore := oldTCPListener != nil && oldTCPListener.RawAddress() == addr
	shouldUDPIgnore := oldUDPListener != nil && oldUDPListener.RawAddress() == addr

	if shouldTCPIgnore && shouldUDPIgnore {
		return nil
	}

	if portIsZero(addr) {
		if oldTCPListener != nil {
			_ = oldTCPListener.Close()
			redirListener = nil
		}
		if oldUDPListener != nil {
			_ = oldUDPListener.Close()
			redirUDPListener = nil
		}
		return nil
	}

	if !shouldTCPIgnore && oldTCPListener != nil && shouldCloseBeforeListen(oldTCPListener.RawAddress(), addr) {
		_ = oldTCPListener.Close()
		redirListener = nil
		oldTCPListener = nil
	}
	if !shouldUDPIgnore && oldUDPListener != nil && shouldCloseBeforeListen(oldUDPListener.RawAddress(), addr) {
		_ = oldUDPListener.Close()
		redirUDPListener = nil
		oldUDPListener = nil
	}

	tcpListener := oldTCPListener
	if !shouldTCPIgnore {
		tcpListener, err = redir.New(addr, tunnel)
		if err != nil {
			return err
		}
	}

	udpListener := oldUDPListener
	if !shouldUDPIgnore {
		udpListener, err = tproxy.NewUDP(addr, tunnel)
		if err != nil {
			log.Warnln("Failed to start Redir UDP Listener: %s", err)
			err = nil
			udpListener = nil
		}
	}

	redirListener = tcpListener
	redirUDPListener = udpListener
	if !shouldTCPIgnore && oldTCPListener != nil {
		_ = oldTCPListener.Close()
	}
	if !shouldUDPIgnore && oldUDPListener != nil {
		_ = oldUDPListener.Close()
	}

	if redirListener == nil {
		return nil
	}

	log.Infoln("Redirect proxy listening at: %s", redirListener.Address())
	return nil
}

func ReCreateShadowSocks(shadowSocksConfig string, tunnel C.Tunnel) error {
	ssMux.Lock()
	defer ssMux.Unlock()

	var err error
	defer func() {
		if err != nil {
			log.Errorln("Start ShadowSocks server error: %s", err.Error())
		}
	}()

	var ssConfig LC.ShadowsocksServer
	if addr, cipher, password, err := embedSS.ParseSSURL(shadowSocksConfig); err == nil {
		ssConfig = LC.ShadowsocksServer{
			Enable:   len(shadowSocksConfig) > 0,
			Listen:   addr,
			Password: password,
			Cipher:   cipher,
			Udp:      true,
		}
	}

	shouldIgnore := false

	oldListener := shadowSocksListener
	if oldListener != nil {
		if oldListener.Config() == ssConfig.String() {
			shouldIgnore = true
		}
	}

	if shouldIgnore {
		return nil
	}

	if !ssConfig.Enable {
		if oldListener != nil {
			_ = oldListener.Close()
			shadowSocksListener = nil
		}
		return nil
	}

	listener, err := sing_shadowsocks.New(ssConfig, inbound.NewListenConfig(), tunnel)
	if err != nil {
		return err
	}

	shadowSocksListener = listener
	if oldListener != nil {
		_ = oldListener.Close()
	}

	for _, addr := range shadowSocksListener.AddrList() {
		log.Infoln("ShadowSocks proxy listening at: %s", addr.String())
	}
	return nil
}

func ReCreateVmess(vmessConfig string, tunnel C.Tunnel) error {
	vmessMux.Lock()
	defer vmessMux.Unlock()

	var err error
	defer func() {
		if err != nil {
			log.Errorln("Start Vmess server error: %s", err.Error())
		}
	}()

	var vsConfig LC.VmessServer
	if addr, username, password, err := sing_vmess.ParseVmessURL(vmessConfig); err == nil {
		vsConfig = LC.VmessServer{
			Enable: len(vmessConfig) > 0,
			Listen: addr,
			Users:  []LC.VmessUser{{Username: username, UUID: password, AlterID: 1}},
		}
	}

	shouldIgnore := false

	oldListener := vmessListener
	if oldListener != nil {
		if oldListener.Config() == vsConfig.String() {
			shouldIgnore = true
		}
	}

	if shouldIgnore {
		return nil
	}

	if !vsConfig.Enable {
		if oldListener != nil {
			_ = oldListener.Close()
			vmessListener = nil
		}
		return nil
	}

	listener, err := sing_vmess.New(vsConfig, inbound.NewListenConfig(), tunnel)
	if err != nil {
		return err
	}

	vmessListener = listener
	if oldListener != nil {
		_ = oldListener.Close()
	}

	for _, addr := range vmessListener.AddrList() {
		log.Infoln("Vmess proxy listening at: %s", addr.String())
	}
	return nil
}

func ReCreateTuic(config LC.TuicServer, tunnel C.Tunnel) error {
	tuicMux.Lock()
	defer tuicMux.Unlock()
	shouldIgnore := false

	var err error
	defer func() {
		if err != nil {
			log.Errorln("Start Tuic server error: %s", err.Error())
		}
	}()

	oldListener := tuicListener
	if oldListener != nil {
		if oldListener.Config().String() == config.String() {
			shouldIgnore = true
		}
	}

	if shouldIgnore {
		return nil
	}

	if !config.Enable {
		if oldListener != nil {
			_ = oldListener.Close()
			tuicListener = nil
		}
		LastTuicConf = config
		return nil
	}

	listener, err := tuic.New(config, inbound.NewListenConfig(), tunnel)
	if err != nil {
		return err
	}

	tuicListener = listener
	LastTuicConf = config
	if oldListener != nil {
		_ = oldListener.Close()
	}

	for _, addr := range tuicListener.AddrList() {
		log.Infoln("Tuic proxy listening at: %s", addr.String())
	}
	return nil
}

func ReCreateTProxy(port int, tunnel C.Tunnel) error {
	tproxyMux.Lock()
	defer tproxyMux.Unlock()

	var err error
	defer func() {
		if err != nil {
			log.Errorln("Start TProxy server error: %s", err.Error())
		}
	}()

	addr := genListenAddr(port)

	oldTCPListener := tproxyListener
	oldUDPListener := tproxyUDPListener
	shouldTCPIgnore := oldTCPListener != nil && oldTCPListener.RawAddress() == addr
	shouldUDPIgnore := oldUDPListener != nil && oldUDPListener.RawAddress() == addr

	if shouldTCPIgnore && shouldUDPIgnore {
		return nil
	}

	if portIsZero(addr) {
		if oldTCPListener != nil {
			_ = oldTCPListener.Close()
			tproxyListener = nil
		}
		if oldUDPListener != nil {
			_ = oldUDPListener.Close()
			tproxyUDPListener = nil
		}
		return nil
	}

	if !shouldTCPIgnore && oldTCPListener != nil && shouldCloseBeforeListen(oldTCPListener.RawAddress(), addr) {
		_ = oldTCPListener.Close()
		tproxyListener = nil
		oldTCPListener = nil
	}
	if !shouldUDPIgnore && oldUDPListener != nil && shouldCloseBeforeListen(oldUDPListener.RawAddress(), addr) {
		_ = oldUDPListener.Close()
		tproxyUDPListener = nil
		oldUDPListener = nil
	}

	tcpListener := oldTCPListener
	if !shouldTCPIgnore {
		tcpListener, err = tproxy.New(addr, tunnel)
		if err != nil {
			return err
		}
	}

	udpListener := oldUDPListener
	if !shouldUDPIgnore {
		udpListener, err = tproxy.NewUDP(addr, tunnel)
		if err != nil {
			log.Warnln("Failed to start TProxy UDP Listener: %s", err)
			err = nil
			udpListener = nil
		}
	}

	tproxyListener = tcpListener
	tproxyUDPListener = udpListener
	if !shouldTCPIgnore && oldTCPListener != nil {
		_ = oldTCPListener.Close()
	}
	if !shouldUDPIgnore && oldUDPListener != nil {
		_ = oldUDPListener.Close()
	}

	if tproxyListener == nil {
		return nil
	}

	log.Infoln("TProxy server listening at: %s", tproxyListener.Address())
	return nil
}

func ReCreateMixed(port int, tunnel C.Tunnel) error {
	mixedMux.Lock()
	defer mixedMux.Unlock()

	var err error
	defer func() {
		if err != nil {
			log.Errorln("Start Mixed(http+socks) server error: %s", err.Error())
		}
	}()

	addr := genListenAddr(port)

	shouldTCPIgnore := false
	shouldUDPIgnore := false
	oldTCPListener := mixedListener
	oldUDPListener := mixedUDPLister

	if oldTCPListener != nil {
		if oldTCPListener.RawAddress() == addr {
			shouldTCPIgnore = true
		}
	}
	if oldUDPListener != nil {
		if oldUDPListener.RawAddress() == addr {
			shouldUDPIgnore = true
		}
	}

	if shouldTCPIgnore && shouldUDPIgnore {
		return nil
	}

	if portIsZero(addr) {
		if oldTCPListener != nil {
			_ = oldTCPListener.Close()
			mixedListener = nil
		}
		if oldUDPListener != nil {
			_ = oldUDPListener.Close()
			mixedUDPLister = nil
		}
		return nil
	}

	if !shouldTCPIgnore && oldTCPListener != nil && shouldCloseBeforeListen(oldTCPListener.RawAddress(), addr) {
		_ = oldTCPListener.Close()
		mixedListener = nil
		oldTCPListener = nil
	}
	if !shouldUDPIgnore && oldUDPListener != nil && shouldCloseBeforeListen(oldUDPListener.RawAddress(), addr) {
		_ = oldUDPListener.Close()
		mixedUDPLister = nil
		oldUDPListener = nil
	}

	tcpListener := oldTCPListener
	if !shouldTCPIgnore {
		tcpListener, err = mixed.New(addr, tunnel)
		if err != nil {
			return err
		}
	}

	udpListener := oldUDPListener
	if !shouldUDPIgnore {
		udpListener, err = socks.NewUDP(addr, tunnel)
		if err != nil {
			if !shouldTCPIgnore {
				_ = tcpListener.Close()
			}
			return err
		}
	}

	mixedListener = tcpListener
	mixedUDPLister = udpListener
	if !shouldTCPIgnore && oldTCPListener != nil {
		_ = oldTCPListener.Close()
	}
	if !shouldUDPIgnore && oldUDPListener != nil {
		_ = oldUDPListener.Close()
	}

	log.Infoln("Mixed(http+socks) proxy listening at: %s", mixedListener.Address())
	return nil
}

func ReCreateTun(tunConf LC.Tun, tunnel C.Tunnel) error {
	tunConf.Sort()

	tunMux.Lock()
	defer tunMux.Unlock()

	var err error
	defer func() {
		if err != nil {
			log.Errorln("Start TUN listening error: %s", err.Error())
		}
	}()

	if tunConf.Equal(LastTunConf) && (tunLister != nil || !tunConf.Enable) {
		if tunLister != nil { // some default value in dialer maybe changed when config reload, reset at here
			if err := tunLister.OnReload(); err != nil {
				return err
			}
		}
		return nil
	}

	if !tunConf.Enable {
		closeTunListener()
		LastTunConf = tunConf
		return nil
	}

	oldListener := tunLister
	// Build the replacement before closing the current TUN so setup failures keep the old adapter alive.
	lister, err := sing_tun.New(tunConf, tunnel)
	if err != nil {
		return err
	}
	tunLister = lister
	LastTunConf = tunConf
	if oldListener != nil {
		_ = oldListener.Close()
	}

	log.Infoln("[TUN] Tun adapter listening at: %s", tunLister.Address())
	return nil
}

func PatchTunnel(tunnels []LC.Tunnel, tunnel C.Tunnel) error {
	tunnelMux.Lock()
	defer tunnelMux.Unlock()

	type addrProxy struct {
		network string
		addr    string
		target  string
		proxy   string
	}

	tcpOld := lo.Map(
		lo.Keys(tunnelTCPListeners),
		func(key string, _ int) addrProxy {
			parts := strings.Split(key, "/")
			return addrProxy{
				network: "tcp",
				addr:    parts[0],
				target:  parts[1],
				proxy:   parts[2],
			}
		},
	)
	udpOld := lo.Map(
		lo.Keys(tunnelUDPListeners),
		func(key string, _ int) addrProxy {
			parts := strings.Split(key, "/")
			return addrProxy{
				network: "udp",
				addr:    parts[0],
				target:  parts[1],
				proxy:   parts[2],
			}
		},
	)
	oldElm := lo.Union(tcpOld, udpOld)

	newElm := lo.FlatMap(
		tunnels,
		func(tunnel LC.Tunnel, _ int) []addrProxy {
			return lo.Map(
				tunnel.Network,
				func(network string, _ int) addrProxy {
					return addrProxy{
						network: network,
						addr:    tunnel.Address,
						target:  tunnel.Target,
						proxy:   tunnel.Proxy,
					}
				},
			)
		},
	)

	needClose, needCreate := lo.Difference(oldElm, newElm)

	createdTCP := map[string]*LT.Listener{}
	createdUDP := map[string]*LT.PacketConn{}
	createdTCPMeta := map[string]addrProxy{}
	createdUDPMeta := map[string]addrProxy{}
	var errs []error
	lc := inbound.NewListenConfig()
	for _, elm := range needCreate {
		key := fmt.Sprintf("%s/%s/%s", elm.addr, elm.target, elm.proxy)
		if elm.network == "tcp" {
			l, err := LT.New(elm.addr, elm.target, elm.proxy, lc, tunnel)
			if err != nil {
				log.Errorln("Start tunnel %s error: %s", elm.target, err.Error())
				errs = append(errs, err)
				continue
			}
			createdTCP[key] = l
			createdTCPMeta[key] = elm
		} else {
			l, err := LT.NewUDP(elm.addr, elm.target, elm.proxy, lc, tunnel)
			if err != nil {
				log.Errorln("Start tunnel %s error: %s", elm.target, err.Error())
				errs = append(errs, err)
				continue
			}
			createdUDP[key] = l
			createdUDPMeta[key] = elm
		}
	}
	if err := errors.Join(errs...); err != nil {
		for _, listener := range createdTCP {
			_ = listener.Close()
		}
		for _, listener := range createdUDP {
			_ = listener.Close()
		}
		return err
	}

	for _, elm := range needClose {
		key := fmt.Sprintf("%s/%s/%s", elm.addr, elm.target, elm.proxy)
		if elm.network == "tcp" {
			tunnelTCPListeners[key].Close()
			delete(tunnelTCPListeners, key)
		} else {
			tunnelUDPListeners[key].Close()
			delete(tunnelUDPListeners, key)
		}
	}

	for key, listener := range createdTCP {
		tunnelTCPListeners[key] = listener
		elm := createdTCPMeta[key]
		log.Infoln("Tunnel(tcp/%s) proxy %s listening at: %s", elm.target, elm.proxy, listener.Address())
	}
	for key, listener := range createdUDP {
		tunnelUDPListeners[key] = listener
		elm := createdUDPMeta[key]
		log.Infoln("Tunnel(udp/%s) proxy %s listening at: %s", elm.target, elm.proxy, listener.Address())
	}
	return nil
}

func PatchInboundListeners(newListenerMap map[string]C.InboundListener, tunnel C.Tunnel, dropOld bool) error {
	inboundMux.Lock()
	defer inboundMux.Unlock()

	started := make(map[string]C.InboundListener)
	for name, newListener := range newListenerMap {
		if oldListener, ok := inboundListeners[name]; ok {
			if oldListener.Config().Equal(newListener.Config()) {
				continue
			}
		}
		if err := newListener.Listen(tunnel); err != nil {
			log.Errorln("Listener %s listen err: %s", name, err.Error())
			for _, listener := range started {
				_ = listener.Close()
			}
			return err
		}
		started[name] = newListener
	}

	for name, newListener := range started {
		if oldListener, ok := inboundListeners[name]; ok {
			_ = oldListener.Close()
		}
		inboundListeners[name] = newListener
	}

	if dropOld {
		for name, oldListener := range inboundListeners {
			if _, ok := newListenerMap[name]; !ok {
				_ = oldListener.Close()
				delete(inboundListeners, name)
			}
		}
	}
	return nil
}

// GetPorts return the ports of proxy servers
func GetPorts() *Ports {
	ports := &Ports{}

	if httpListener != nil {
		_, portStr, _ := net.SplitHostPort(httpListener.Address())
		port, _ := strconv.Atoi(portStr)
		ports.Port = port
	}

	if socksListener != nil {
		_, portStr, _ := net.SplitHostPort(socksListener.Address())
		port, _ := strconv.Atoi(portStr)
		ports.SocksPort = port
	}

	if redirListener != nil {
		_, portStr, _ := net.SplitHostPort(redirListener.Address())
		port, _ := strconv.Atoi(portStr)
		ports.RedirPort = port
	}

	if tproxyListener != nil {
		_, portStr, _ := net.SplitHostPort(tproxyListener.Address())
		port, _ := strconv.Atoi(portStr)
		ports.TProxyPort = port
	}

	if mixedListener != nil {
		_, portStr, _ := net.SplitHostPort(mixedListener.Address())
		port, _ := strconv.Atoi(portStr)
		ports.MixedPort = port
	}

	if shadowSocksListener != nil {
		ports.ShadowSocksConfig = shadowSocksListener.Config()
	}

	if vmessListener != nil {
		ports.VmessConfig = vmessListener.Config()
	}

	return ports
}

func portIsZero(addr string) bool {
	_, port, err := net.SplitHostPort(addr)
	if port == "0" || port == "" || err != nil {
		return true
	}
	return false
}

func genAddr(host string, port int, allowLan bool) string {
	if allowLan {
		if host == "*" {
			return fmt.Sprintf(":%d", port)
		}
		return fmt.Sprintf("%s:%d", host, port)
	}

	return fmt.Sprintf("127.0.0.1:%d", port)
}

func closeTunListener() {
	if tunLister != nil {
		tunLister.Close()
		tunLister = nil
	}
}

func Cleanup() {
	closeTunListener()
}

func StopListener() {
	httpMux.Lock()
	if httpListener != nil {
		httpListener.Close()
		httpListener = nil
	}
	httpMux.Unlock()

	socksMux.Lock()
	if socksListener != nil {
		socksListener.Close()
		socksListener = nil
	}
	if socksUDPListener != nil {
		socksUDPListener.Close()
		socksUDPListener = nil
	}
	socksMux.Unlock()

	redirMux.Lock()
	if redirListener != nil {
		redirListener.Close()
		redirListener = nil
	}
	if redirUDPListener != nil {
		redirUDPListener.Close()
		redirUDPListener = nil
	}
	redirMux.Unlock()

	tproxyMux.Lock()
	if tproxyListener != nil {
		tproxyListener.Close()
		tproxyListener = nil
	}
	if tproxyUDPListener != nil {
		tproxyUDPListener.Close()
		tproxyUDPListener = nil
	}
	tproxyMux.Unlock()

	mixedMux.Lock()
	if mixedListener != nil {
		mixedListener.Close()
		mixedListener = nil
	}
	if mixedUDPLister != nil {
		mixedUDPLister.Close()
		mixedUDPLister = nil
	}
	mixedMux.Unlock()

	tunMux.Lock()
	closeTunListener()
	tunMux.Unlock()

	ssMux.Lock()
	if shadowSocksListener != nil {
		shadowSocksListener.Close()
		shadowSocksListener = nil
	}
	ssMux.Unlock()

	vmessMux.Lock()
	if vmessListener != nil {
		vmessListener.Close()
		vmessListener = nil
	}
	vmessMux.Unlock()

	tuicMux.Lock()
	if tuicListener != nil {
		tuicListener.Close()
		tuicListener = nil
	}
	tuicMux.Unlock()

	inboundMux.Lock()
	for name, l := range inboundListeners {
		l.Close()
		delete(inboundListeners, name)
	}
	inboundMux.Unlock()
}
