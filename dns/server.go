package dns

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/metacubex/mihomo/common/sockopt"
	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	D "github.com/miekg/dns"
)

var (
	address  string
	server   = &Server{}
	serverMu sync.Mutex

	dnsDefaultTTL uint32 = 600
)

type Server struct {
	serviceMu sync.RWMutex
	service   resolver.Service
	tcpServer *D.Server
	udpServer *D.Server
}

// ServeDNS implement D.Handler ServeDNS
func (s *Server) ServeDNS(w D.ResponseWriter, r *D.Msg) {
	s.serviceMu.RLock()
	service := s.service
	s.serviceMu.RUnlock()
	if service == nil {
		m := new(D.Msg)
		m.SetRcode(r, D.RcodeServerFailure)
		w.WriteMsg(m)
		return
	}
	msg, err := service.ServeMsg(context.Background(), r)
	if err != nil {
		m := new(D.Msg)
		m.SetRcode(r, D.RcodeServerFailure)
		// does not matter if this write fails
		w.WriteMsg(m)
		return
	}
	msg.Compress = true
	w.WriteMsg(msg)
}

func (s *Server) SetService(service resolver.Service) {
	s.serviceMu.Lock()
	defer s.serviceMu.Unlock()
	s.service = service
}

func ReCreateServer(addr string, lc C.InboundListenConfig, service resolver.Service) error {
	serverMu.Lock()
	defer serverMu.Unlock()

	if addr == address && service != nil {
		server.SetService(service)
		return nil
	}

	oldServer := server

	if addr == "" || lc == nil || service == nil {
		server = &Server{}
		address = ""
		shutdownServer(oldServer)
		return nil
	}

	var err error
	defer func() {
		if err != nil {
			log.Errorln("Start DNS server error: %s", err.Error())
		}
	}()

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid DNS server listen address %s: %w", addr, err)
	}
	if port == "0" || port == "" {
		server = &Server{}
		address = ""
		shutdownServer(oldServer)
		return nil
	}

	if shouldCloseBeforeListen(address, addr) {
		server = &Server{}
		address = ""
		shutdownServer(oldServer)
		oldServer = nil
	}

	p, err := lc.ListenPacket(context.Background(), "udp", addr)
	if err != nil {
		log.Errorln("Start DNS server(UDP) error: %s", err.Error())
		return err
	}

	if err := sockopt.UDPReuseaddr(p); err != nil {
		log.Warnln("Failed to Reuse UDP Address: %s", err)
	}

	l, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		_ = p.Close()
		log.Errorln("Start DNS server(TCP) error: %s", err.Error())
		return err
	}

	address = addr
	newServer := &Server{service: service}
	newServer.udpServer = &D.Server{Addr: addr, PacketConn: p, Handler: newServer}
	newServer.tcpServer = &D.Server{Addr: addr, Listener: l, Handler: newServer}
	server = newServer
	shutdownServer(oldServer)

	go func() {
		log.Infoln("DNS server(UDP) listening at: %s", p.LocalAddr().String())
		_ = newServer.udpServer.ActivateAndServe()
	}()

	go func() {
		log.Infoln("DNS server(TCP) listening at: %s", l.Addr().String())
		_ = newServer.tcpServer.ActivateAndServe()
	}()
	return nil
}

func shouldCloseBeforeListen(oldAddr string, newAddr string) bool {
	if oldAddr == "" || oldAddr == newAddr {
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
	switch host {
	case "", "*", "0.0.0.0", "::":
		return true
	default:
		return false
	}
}

func shutdownServer(server *Server) {
	if server == nil {
		return
	}
	if server.tcpServer != nil {
		_ = server.tcpServer.Shutdown()
	}
	if server.udpServer != nil {
		_ = server.udpServer.Shutdown()
	}
}
