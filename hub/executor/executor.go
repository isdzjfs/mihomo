package executor

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
	_ "unsafe"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/mihomo/adapter/outboundgroup"
	"github.com/metacubex/mihomo/component/auth"
	"github.com/metacubex/mihomo/component/ca"
	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/geodata"
	mihomoHttp "github.com/metacubex/mihomo/component/http"
	"github.com/metacubex/mihomo/component/iface"
	"github.com/metacubex/mihomo/component/keepalive"
	"github.com/metacubex/mihomo/component/profile"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/resource"
	"github.com/metacubex/mihomo/component/sniffer"
	"github.com/metacubex/mihomo/component/trie"
	"github.com/metacubex/mihomo/component/updater"
	"github.com/metacubex/mihomo/config"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/dns"
	"github.com/metacubex/mihomo/listener"
	authStore "github.com/metacubex/mihomo/listener/auth"
	LC "github.com/metacubex/mihomo/listener/config"
	"github.com/metacubex/mihomo/listener/inner"
	"github.com/metacubex/mihomo/listener/tproxy"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/ntp/ntp"
	"github.com/metacubex/mihomo/tunnel"
)

var mux sync.Mutex

func readConfig(path string) ([]byte, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("configuration file %s is empty", path)
	}

	return data, err
}

// Parse config with default config path
func Parse() (*config.Config, error) {
	return ParseWithPath(C.Path.Config())
}

// ParseWithPath parse config with custom config path
func ParseWithPath(path string) (*config.Config, error) {
	buf, err := readConfig(path)
	if err != nil {
		return nil, err
	}

	return ParseWithBytes(buf)
}

// ParseWithBytes config with buffer
func ParseWithBytes(buf []byte) (*config.Config, error) {
	return config.Parse(buf)
}

// ApplyConfig dispatch configure to all parts without ExternalController
func ApplyConfig(cfg *config.Config, force bool) error {
	mux.Lock()
	defer mux.Unlock()
	log.SetLevel(cfg.General.LogLevel)
	if err := log.SetFileOutput(cfg.General.LogFile); err != nil {
		log.Warnln("Failed to set log file output: %v", err)
	}

	tunnel.OnSuspend()
	var applyErrs []error
	appendErr := func(name string, err error) {
		if err != nil {
			applyErrs = append(applyErrs, fmt.Errorf("%s: %w", name, err))
		}
	}

	ca.ResetCertificate()
	for _, c := range cfg.TLS.CustomTrustCert {
		if err := ca.AddCertificate(c); err != nil {
			log.Warnln("%s\nadd error: %s", c, err.Error())
		}
	}

	updateExperimental(cfg.Experimental)
	updateUsers(cfg.Users)
	oldProxies, oldProviders := updateProxies(cfg.Proxies, cfg.Providers)
	oldRuleProviders := updateRules(cfg.Rules, cfg.SubRules, cfg.RuleProviders)
	updateSniffer(cfg.Sniffer)
	updateHosts(cfg.Hosts)
	updateGeneral(cfg.General, true)
	updateNTP(cfg.NTP)
	appendErr("DNS", updateDNS(cfg.DNS, cfg.General.IPv6))
	appendErr("listeners", updateListeners(cfg.General, cfg.Listeners, force))
	appendErr("TUN", updateTun(cfg.General)) // tun should not care "force"
	appendErr("iptables", updateIPTables(cfg))
	appendErr("tunnels", updateTunnels(cfg.Tunnels))

	tunnel.OnInnerLoading()

	initInnerTcp()
	appendErr("proxy providers", loadProvider(cfg.Providers))
	updateProfile(cfg)
	appendErr("rule providers", loadProvider(cfg.RuleProviders))
	closeReplacedProviders(oldProviders, cfg.Providers)
	closeReplacedProviders(oldRuleProviders, cfg.RuleProviders)
	closeReplacedProxies(oldProxies, cfg.Proxies)
	runtime.GC()
	tunnel.OnRunning()
	updateUpdater(cfg)

	resolver.ResetConnection()
	return errors.Join(applyErrs...)
}

func initInnerTcp() {
	inner.New(tunnel.Tunnel)
}

func GetGeneral() *config.General {
	ports := listener.GetPorts()
	var authenticator []string
	if auth := authStore.Default.Authenticator(); auth != nil {
		authenticator = auth.Users()
	}

	general := &config.General{
		Inbound: config.Inbound{
			Port:              ports.Port,
			SocksPort:         ports.SocksPort,
			RedirPort:         ports.RedirPort,
			TProxyPort:        ports.TProxyPort,
			MixedPort:         ports.MixedPort,
			Tun:               listener.GetTunConf(),
			TuicServer:        listener.GetTuicConf(),
			ShadowSocksConfig: ports.ShadowSocksConfig,
			VmessConfig:       ports.VmessConfig,
			Authentication:    authenticator,
			SkipAuthPrefixes:  inbound.SkipAuthPrefixes(),
			LanAllowedIPs:     inbound.AllowedIPs(),
			LanDisAllowedIPs:  inbound.DisAllowedIPs(),
			AllowLan:          listener.AllowLan(),
			BindAddress:       listener.BindAddress(),
			InboundTfo:        inbound.Tfo(),
			InboundMPTCP:      inbound.MPTCP(),
		},
		Mode:         tunnel.Mode(),
		UnifiedDelay: adapter.UnifiedDelay.Load(),
		LogLevel:     log.Level(),
		IPv6:         !resolver.DisableIPv6Value(),
		Interface:    dialer.DefaultInterface.Load(),
		RoutingMark:  int(dialer.DefaultRoutingMark.Load()),
		GeoXUrl: config.GeoXUrl{
			GeoIp:   geodata.GeoIpUrl(),
			Mmdb:    geodata.MmdbUrl(),
			ASN:     geodata.ASNUrl(),
			GeoSite: geodata.GeoSiteUrl(),
		},
		GeoAutoUpdate:     updater.GeoAutoUpdate(),
		GeoUpdateInterval: updater.GeoUpdateInterval(),
		GeodataMode:       geodata.GeodataMode(),
		GeodataLoader:     geodata.LoaderName(),
		GeositeMatcher:    geodata.SiteMatcherName(),
		TCPConcurrent:     dialer.GetTcpConcurrent(),
		FindProcessMode:   tunnel.FindProcessMode(),
		Sniffing:          tunnel.IsSniffing(),
		GlobalUA:          mihomoHttp.UA(),
		ETagSupport:       resource.ETag(),
		KeepAliveInterval: int(keepalive.KeepAliveInterval() / time.Second),
		KeepAliveIdle:     int(keepalive.KeepAliveIdle() / time.Second),
		DisableKeepAlive:  keepalive.DisableKeepAlive(),
	}

	return general
}

func updateListeners(general *config.General, listeners map[string]C.InboundListener, force bool) error {
	var errs []error
	appendErr := func(name string, err error) {
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
	}

	appendErr("inbound listeners", listener.PatchInboundListeners(listeners, tunnel.Tunnel, true))
	if !force {
		return errors.Join(errs...)
	}

	allowLan := general.AllowLan
	listener.SetAllowLan(allowLan)
	inbound.SetSkipAuthPrefixes(general.SkipAuthPrefixes)
	inbound.SetAllowedIPs(general.LanAllowedIPs)
	inbound.SetDisAllowedIPs(general.LanDisAllowedIPs)

	bindAddress := general.BindAddress
	listener.SetBindAddress(bindAddress)
	appendErr("HTTP", listener.ReCreateHTTP(general.Port, tunnel.Tunnel))
	appendErr("SOCKS", listener.ReCreateSocks(general.SocksPort, tunnel.Tunnel))
	appendErr("redir", listener.ReCreateRedir(general.RedirPort, tunnel.Tunnel))
	appendErr("TProxy", listener.ReCreateTProxy(general.TProxyPort, tunnel.Tunnel))
	appendErr("mixed", listener.ReCreateMixed(general.MixedPort, tunnel.Tunnel))
	appendErr("ShadowSocks", listener.ReCreateShadowSocks(general.ShadowSocksConfig, tunnel.Tunnel))
	appendErr("Vmess", listener.ReCreateVmess(general.VmessConfig, tunnel.Tunnel))
	appendErr("Tuic", listener.ReCreateTuic(general.TuicServer, tunnel.Tunnel))
	return errors.Join(errs...)
}

func updateTun(general *config.General) error {
	return listener.ReCreateTun(general.Tun, tunnel.Tunnel)
}

func updateExperimental(c *config.Experimental) {
	if c.QUICGoDisableGSO {
		_ = os.Setenv("QUIC_GO_DISABLE_GSO", strconv.FormatBool(true))
	}
	if c.QUICGoDisableECN {
		_ = os.Setenv("QUIC_GO_DISABLE_ECN", strconv.FormatBool(true))
	}
	resolver.SetIP4PEnable(c.IP4PEnable)
}

func updateNTP(c *config.NTP) {
	if c.Enable {
		ntp.ReCreateNTPService(
			net.JoinHostPort(c.Server, strconv.Itoa(c.Port)),
			time.Duration(c.Interval),
			c.DialerProxy,
			tunnel.Tunnel,
			c.WriteToSystem,
		)
	} else {
		ntp.ReCreateNTPService("", 0, "", nil, false)
	}
}

func updateDNS(c *config.DNS, generalIPv6 bool) error {
	if !c.Enable {
		resolver.SetDefaultResolver(nil)
		resolver.SetDefaultHostMapper(nil)
		resolver.SetDefaultService(nil)
		resolver.SetProxyServerHostResolver(nil)
		resolver.SetDirectHostResolver(nil)
		return dns.ReCreateServer("", nil, nil)
	}

	ipv6 := c.IPv6 && generalIPv6
	r := dns.NewResolver(dns.Config{
		Main:                 c.NameServer,
		Fallback:             c.Fallback,
		IPv6:                 ipv6,
		IPv6Timeout:          c.IPv6Timeout,
		FallbackIPFilter:     c.FallbackIPFilter,
		FallbackDomainFilter: c.FallbackDomainFilter,
		FallbackLazyQuery:    c.FallbackLazyQuery,
		Default:              c.DefaultNameserver,
		Policy:               c.NameServerPolicy,
		ProxyServer:          c.ProxyServerNameserver,
		ProxyServerPolicy:    c.ProxyServerPolicy,
		DirectServer:         c.DirectNameServer,
		DirectFollowPolicy:   c.DirectFollowPolicy,
		CacheAlgorithm:       c.CacheAlgorithm,
		CacheMaxSize:         c.CacheMaxSize,
	})
	m := dns.NewEnhancer(dns.EnhancerConfig{
		IPv6:          ipv6,
		EnhancedMode:  c.EnhancedMode,
		FakeIPPool:    c.FakeIPPool,
		FakeIPPool6:   c.FakeIPPool6,
		FakeIPSkipper: c.FakeIPSkipper,
		FakeIPTTL:     c.FakeIPTTL,
		UseHosts:      c.UseHosts,
	})

	// reuse cache of old host mapper
	if old, ok := resolver.DefaultHostMapperValue().(*dns.ResolverEnhancer); ok {
		m.PatchFrom(old)
	}

	s := dns.NewService(r, m)

	lc := inbound.NewListenConfig()
	lc.SetRouteMark(c.ListenRoutingMark)
	if err := dns.ReCreateServer(c.Listen, lc, s); err != nil {
		return err
	}

	resolver.SetDefaultResolver(r)
	resolver.SetDefaultHostMapper(m)
	resolver.SetDefaultService(s)
	resolver.SetUseSystemHosts(c.UseSystemHosts)

	if r.ProxyResolver.Invalid() {
		resolver.SetProxyServerHostResolver(r.ProxyResolver)
	} else {
		resolver.SetProxyServerHostResolver(r.Resolver)
	}

	if r.DirectResolver.Invalid() {
		resolver.SetDirectHostResolver(r.DirectResolver)
	} else {
		resolver.SetDirectHostResolver(r.Resolver)
	}
	return nil
}

func updateHosts(tree *trie.DomainTrie[resolver.HostValue]) {
	resolver.SetDefaultHosts(resolver.NewHosts(tree))
}

func updateProxies(proxies map[string]C.Proxy, providers map[string]P.ProxyProvider) (map[string]C.Proxy, map[string]P.ProxyProvider) {
	return tunnel.UpdateProxies(proxies, providers)
}

func updateRules(rules []C.Rule, subRules map[string][]C.Rule, ruleProviders map[string]P.RuleProvider) map[string]P.RuleProvider {
	return tunnel.UpdateRules(rules, subRules, ruleProviders)
}

type closeableProvider interface {
	Close() error
}

func closeReplacedProviders[T P.Provider](oldProviders map[string]T, currentProviders map[string]T) {
	for name, oldProvider := range oldProviders {
		if currentProvider, ok := currentProviders[name]; ok {
			var oldProviderInterface P.Provider = oldProvider
			var currentProviderInterface P.Provider = currentProvider
			if oldProviderInterface == currentProviderInterface {
				continue
			}
		}

		closer, ok := any(oldProvider).(closeableProvider)
		if !ok {
			continue
		}
		if err := closer.Close(); err != nil {
			log.Warnln("close provider %s failed: %v", oldProvider.Name(), err)
		}
	}
}

func closeReplacedProxies(oldProxies map[string]C.Proxy, currentProxies map[string]C.Proxy) {
	for name, oldProxy := range oldProxies {
		if currentProxy, ok := currentProxies[name]; ok && oldProxy == currentProxy {
			continue
		}
		if err := oldProxy.Close(); err != nil {
			log.Warnln("close proxy %s failed: %v", oldProxy.Name(), err)
		}
	}
}

func loadProvider[T P.Provider](providers map[string]T) error {
	var errs []error
	errMux := sync.Mutex{}
	load := func(pv T) {
		name := pv.Name()
		if pv.VehicleType() == P.Compatible {
			log.Infoln("Start initial compatible provider %s", name)
		} else {
			log.Infoln("Start initial provider %s", name)
		}

		if err := pv.Initial(); err != nil {
			switch pv.Type() {
			case P.Proxy:
				{
					log.Errorln("initial proxy provider %s error: %v", name, err)
				}
			case P.Rule:
				{
					log.Errorln("initial rule provider %s error: %v", name, err)
				}
			}
			errMux.Lock()
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			errMux.Unlock()
		}
	}

	wg := sync.WaitGroup{}
	ch := make(chan struct{}, concurrentCount)
	for _, pv := range providers {
		pv := pv
		wg.Add(1)
		ch <- struct{}{}
		go func() {
			defer func() { <-ch; wg.Done() }()
			load(pv)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func updateSniffer(snifferConfig *sniffer.Config) {
	dispatcher, err := sniffer.NewDispatcher(snifferConfig)
	if err != nil {
		log.Warnln("initial sniffer failed, err:%v", err)
	}

	tunnel.UpdateSniffer(dispatcher)

	if snifferConfig.Enable {
		log.Infoln("Sniffer is loaded and working")
	} else {
		log.Infoln("Sniffer is closed")
	}
}

func updateTunnels(tunnels []LC.Tunnel) error {
	return listener.PatchTunnel(tunnels, tunnel.Tunnel)
}

func updateUpdater(cfg *config.Config) {
	general := cfg.General
	updater.ConfigureGeoUpdater(general.GeoAutoUpdate, general.GeoUpdateInterval)

	controller := cfg.Controller
	updater.DefaultUiUpdater = updater.NewUiUpdater(controller.ExternalUI, controller.ExternalUIURL, controller.ExternalUIName)
	updater.DefaultUiUpdater.AutoDownloadUI()
}

//go:linkname temporaryUpdateGeneral github.com/metacubex/mihomo/config.temporaryUpdateGeneral
func temporaryUpdateGeneral(general *config.General) func() {
	oldGeneral := GetGeneral()
	updateGeneral(general, false)
	return func() {
		updateGeneral(oldGeneral, false)
	}
}

func updateGeneral(general *config.General, logging bool) {
	tunnel.SetMode(general.Mode)
	tunnel.SetFindProcessMode(general.FindProcessMode)
	resolver.SetDisableIPv6(!general.IPv6)

	dialer.SetTcpConcurrent(general.TCPConcurrent)
	if logging && general.TCPConcurrent {
		log.Infoln("Use tcp concurrent")
	}

	inbound.SetTfo(general.InboundTfo)
	inbound.SetMPTCP(general.InboundMPTCP)

	keepalive.SetKeepAliveIdle(time.Duration(general.KeepAliveIdle) * time.Second)
	keepalive.SetKeepAliveInterval(time.Duration(general.KeepAliveInterval) * time.Second)
	keepalive.SetDisableKeepAlive(general.DisableKeepAlive)

	adapter.UnifiedDelay.Store(general.UnifiedDelay)

	dialer.DefaultInterface.Store(general.Interface)
	dialer.DefaultRoutingMark.Store(int32(general.RoutingMark))
	if logging && general.RoutingMark > 0 {
		log.Infoln("Use routing mark: %#x", general.RoutingMark)
	}

	iface.FlushCache()

	geodata.SetGeodataMode(general.GeodataMode)
	geodata.SetLoader(general.GeodataLoader)
	geodata.SetSiteMatcher(general.GeositeMatcher)
	geodata.SetGeoIpUrl(general.GeoXUrl.GeoIp)
	geodata.SetGeoSiteUrl(general.GeoXUrl.GeoSite)
	geodata.SetMmdbUrl(general.GeoXUrl.Mmdb)
	geodata.SetASNUrl(general.GeoXUrl.ASN)
	mihomoHttp.SetUA(general.GlobalUA)
	resource.SetETag(general.ETagSupport)
}

func updateUsers(users []auth.AuthUser) {
	authenticator := auth.NewAuthenticator(users)
	authStore.Default.SetAuthenticator(authenticator)
	if authenticator != nil {
		log.Infoln("Authentication of local server updated")
	}
}

func updateProfile(cfg *config.Config) {
	profileCfg := cfg.Profile

	profile.StoreSelected.Store(profileCfg.StoreSelected)
	if profileCfg.StoreSelected {
		patchSelectGroup(cfg.Proxies)
	}
}

func patchSelectGroup(proxies map[string]C.Proxy) {
	mapping := cachefile.Cache().SelectedMap()
	if mapping == nil {
		return
	}

	for name, outbound := range proxies {
		selector, ok := outbound.Adapter().(outboundgroup.SelectAble)
		if !ok {
			continue
		}

		selected, exist := mapping[name]
		if !exist {
			continue
		}

		selector.ForceSet(selected)
	}
}

func updateIPTables(cfg *config.Config) error {
	iptables := cfg.IPTables
	if runtime.GOOS != "linux" {
		return nil
	}
	if !iptables.Enable {
		tproxy.CleanupTProxyIPTables()
		return nil
	}

	if cfg.General.Tun.Enable {
		return fmt.Errorf("when tun is enabled, iptables cannot be set automatically")
	}

	var (
		inboundInterface = "lo"
		bypass           = iptables.Bypass
		tProxyPort       = cfg.General.TProxyPort
		dnsCfg           = cfg.DNS
		DnsRedirect      = iptables.DnsRedirect

		dnsPort netip.AddrPort
	)

	if tProxyPort == 0 {
		return fmt.Errorf("tproxy-port must be greater than zero")
	}

	if DnsRedirect {
		if !dnsCfg.Enable {
			return fmt.Errorf("DNS server must be enable")
		}

		parsedDNSPort, err := netip.ParseAddrPort(dnsCfg.Listen)
		if err != nil {
			return fmt.Errorf("DNS server must be correct: %w", err)
		}
		dnsPort = parsedDNSPort
	}

	if iptables.InboundInterface != "" {
		inboundInterface = iptables.InboundInterface
	}

	tproxy.CleanupTProxyIPTables()
	dialer.DefaultRoutingMark.CompareAndSwap(0, 2158)
	if err := tproxy.SetTProxyIPTables(inboundInterface, bypass, uint16(tProxyPort), DnsRedirect, dnsPort.Port()); err != nil {
		log.Errorln("[IPTABLES] setting iptables failed: %s", err.Error())
		return err
	}

	log.Infoln("[IPTABLES] Setting iptables completed")
	return nil
}

func Shutdown() {
	listener.Cleanup()
	tproxy.CleanupTProxyIPTables()
	resolver.StoreFakePoolState()

	log.Warnln("Mihomo shutting down")
}
