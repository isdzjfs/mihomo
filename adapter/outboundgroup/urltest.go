package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/singledo"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

type URLTestOption struct {
	Tolerance uint16 `group:"tolerance,omitempty"`
}

type URLTest struct {
	*GroupBase
	stateMux       sync.RWMutex
	selected       string
	testUrl        string
	expectedStatus string
	tolerance      uint16
	disableUDP     bool
	fastNode       C.Proxy
	fastSingle     *singledo.Single[C.Proxy]
}

const maxURLTestDialAttempts = 2

func (u *URLTest) Now() string {
	return u.fast(false).Name()
}

func (u *URLTest) Set(name string) error {
	var p C.Proxy
	for _, proxy := range u.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}
	if p == nil {
		return errors.New("proxy not exist")
	}
	u.ForceSet(name)
	return nil
}

func (u *URLTest) ForceSet(name string) {
	u.stateMux.Lock()
	u.selected = name
	u.stateMux.Unlock()
	u.fastSingle.Reset()
}

// DialContext implements C.ProxyAdapter
func (u *URLTest) DialContext(ctx context.Context, metadata *C.Metadata) (c C.Conn, err error) {
	var staleErr error
	for attempt := 0; attempt < maxURLTestDialAttempts; attempt++ {
		proxy := u.fast(true)

		u.stateMux.RLock()
		selected := u.selected
		u.stateMux.RUnlock()
		manualSelected := selected != "" && proxy.Name() == selected

		log.Debugln("URLTest [%s] selected node [%s] (Alive: %t, ManualSelected: %t) for destination [%s]", u.Name(), proxy.Name(), proxy.AliveForTestUrl(u.testUrl), manualSelected, metadata.String())

		c, err = proxy.DialContext(ctx, metadata)
		if err == nil {
			if !u.isCurrentFastNode(proxy) {
				_ = c.Close()
				u.fastSingle.Reset()
				staleErr = fmt.Errorf("URLTest [%s] selected node [%s] is stale", u.Name(), proxy.Name())
				log.Debugln("%v", staleErr)
				continue
			}
			c.AppendToChains(u)
		} else {
			log.Debugln("URLTest [%s] dial node [%s] failed: %v", u.Name(), proxy.Name(), err)
			if !manualSelected {
				u.fastSingle.Reset()
				u.onDialFailed(proxy.Type(), err, u.healthCheck)
			}
			return c, err
		}

		// Bypass the health tracking wrapper for manually selected nodes or VLESS nodes.
		// For VLESS with Vision/Reality flow, WriteBuffer() internally replaces its
		// ExtendedWriter during the TLS handshake phase. Intercepting the first write
		// via the wrapper conflicts with this dynamic writer replacement and breaks
		// the Vision flow protocol.
		bypassWrapper := manualSelected || proxy.Type() == C.Vless
		if !bypassWrapper && N.NeedHandshake(c) {
			c = callback.NewFirstWriteCallBackConn(c, func(err error) {
				if err == nil {
					u.onDialSuccess()
				} else {
					log.Debugln("URLTest [%s] handshake node [%s] failed: %v", u.Name(), proxy.Name(), err)
					u.onDialFailed(proxy.Type(), err, u.healthCheck)
				}
			})
		} else if bypassWrapper {
			log.Debugln("URLTest [%s] bypassing health tracking wrapper for node [%s] (Reason: ManualSelected=%t, Type=%s)", u.Name(), proxy.Name(), manualSelected, proxy.Type().String())
		}

		return c, nil
	}

	return nil, staleErr
}

// ListenPacketContext implements C.ProxyAdapter
func (u *URLTest) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	var staleErr error
	for attempt := 0; attempt < maxURLTestDialAttempts; attempt++ {
		proxy := u.fast(true)

		u.stateMux.RLock()
		selected := u.selected
		u.stateMux.RUnlock()
		manualSelected := selected != "" && proxy.Name() == selected

		pc, err := proxy.ListenPacketContext(ctx, metadata)
		if err == nil {
			if !u.isCurrentFastNode(proxy) {
				_ = pc.Close()
				u.fastSingle.Reset()
				staleErr = fmt.Errorf("URLTest [%s] selected packet node [%s] is stale", u.Name(), proxy.Name())
				log.Debugln("%v", staleErr)
				continue
			}
			pc.AppendToChains(u)
			return pc, nil
		} else {
			log.Debugln("URLTest [%s] ListenPacket node [%s] failed: %v", u.Name(), proxy.Name(), err)
			if !manualSelected && proxy.Type() != C.Vless {
				u.fastSingle.Reset()
				u.onDialFailed(proxy.Type(), err, u.healthCheck)
			}
			return pc, err
		}
	}

	return nil, staleErr
}

// Unwrap implements C.ProxyAdapter
func (u *URLTest) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return u.fast(touch)
}

func (u *URLTest) healthCheck() {
	u.fastSingle.Reset()
	u.GroupBase.healthCheck()
	u.fastSingle.Reset()
	_ = u.fastWithHealthCheck(false, false) // preheat without recursively triggering another health check
}

func (u *URLTest) fast(touch bool) C.Proxy {
	return u.fastWithHealthCheck(touch, true)
}

func (u *URLTest) fastWithHealthCheck(touch bool, triggerHealthCheck bool) C.Proxy {
	elm, _, shared := u.fastSingle.Do(func() (C.Proxy, error) {
		proxies := u.GetProxies(touch)
		selected, fastNode := u.snapshotState()

		if selected != "" {
			selectedFound := false
			for _, proxy := range proxies {
				if proxy.Name() == selected {
					selectedFound = true
					if !proxy.AliveForTestUrl(u.testUrl) {
						log.Debugln("URLTest [%s] manual selected node [%s] is not alive, falling back to auto", u.Name(), selected)
						break
					}
					log.Debugln("URLTest [%s] using manual selected node [%s]", u.Name(), selected)
					u.setFastNode(proxy, false)
					return proxy, nil
				}
			}
			if !selectedFound {
				log.Debugln("URLTest [%s] manual selected node [%s] not found in proxies, falling back to auto", u.Name(), selected)
			}
		}

		var (
			oldFastNode       = fastNode
			fast              C.Proxy
			fastDelay         uint16
			hasAliveFast      bool
			fastNotExist      = true
			currentFastAlive  bool
			currentFastDelay  uint16
			closeStaleCurrent bool
		)

		for _, proxy := range proxies {
			alive := proxy.AliveForTestUrl(u.testUrl)
			isCurrentFast := fastNode != nil && proxy.Name() == fastNode.Name()
			if isCurrentFast {
				fastNotExist = false
				currentFastAlive = alive
			}

			if !alive {
				log.Debugln("URLTest [%s] skip node [%s] because it's not alive", u.Name(), proxy.Name())
				continue
			}

			delay := proxy.LastDelayForTestUrl(u.testUrl)
			if isCurrentFast {
				currentFastDelay = delay
			}
			if !hasAliveFast || delay < fastDelay {
				fast = proxy
				fastDelay = delay
				hasAliveFast = true
			}
		}

		// Do not fall back to timeout nodes when at least one alive node exists.
		if hasAliveFast {
			if fastNode == nil || fastNotExist || !currentFastAlive || currentFastDelay > fastDelay+u.tolerance {
				fastNode = fast
			}
		} else if fastNode == nil || fastNotExist || !currentFastAlive {
			fastNode = proxies[0]
			closeStaleCurrent = oldFastNode != nil && !currentFastAlive
			if triggerHealthCheck {
				// all nodes are dead, trigger async health check to recover
				go u.healthCheck()
			}
		}

		u.setFastNode(fastNode, closeStaleCurrent)
		return fastNode, nil
	})
	if shared && touch {
		u.Touch()
	}

	return elm
}

func (u *URLTest) snapshotState() (string, C.Proxy) {
	u.stateMux.RLock()
	defer u.stateMux.RUnlock()
	return u.selected, u.fastNode
}

func (u *URLTest) getSelected() string {
	u.stateMux.RLock()
	defer u.stateMux.RUnlock()
	return u.selected
}

func (u *URLTest) isCurrentFastNode(proxy C.Proxy) bool {
	_, fastNode := u.snapshotState()
	return fastNode == nil || proxy == nil || fastNode.Name() == proxy.Name()
}

func (u *URLTest) closeConnections() {
	groupName := u.Name()
	statistic.DefaultManager.Range(func(c statistic.Tracker) bool {
		for _, chain := range c.Chains() {
			if chain == groupName {
				_ = c.Close()
				break
			}
		}
		return true
	})
}

func (u *URLTest) setFastNode(proxy C.Proxy, closeStaleCurrent bool) {
	u.stateMux.Lock()
	old := u.fastNode
	u.fastNode = proxy
	u.stateMux.Unlock()

	// Close connections still using the old (stale) node so they re-dial
	// through the newly selected node. Only connections that pass through
	// this URLTest group are affected; direct/other-group connections are not.
	if old != nil && proxy != nil && (old.Name() != proxy.Name() || closeStaleCurrent) {
		u.closeConnections()
	}
}

// SupportUDP implements C.ProxyAdapter
func (u *URLTest) SupportUDP() bool {
	if u.disableUDP {
		return false
	}
	return u.fast(false).SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (u *URLTest) IsL3Protocol(metadata *C.Metadata) bool {
	return u.fast(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (u *URLTest) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range u.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	return json.Marshal(map[string]any{
		"type":           u.Type().String(),
		"now":            u.Now(),
		"all":            all,
		"testUrl":        u.testUrl,
		"expectedStatus": u.expectedStatus,
		"fixed":          u.getSelected(),
		"hidden":         u.Hidden(),
		"icon":           u.Icon(),
		"emptyFallback":  u.EmptyFallback().Name(),
	})
}

func (u *URLTest) Providers() []P.ProxyProvider {
	return u.providers
}

func (u *URLTest) Proxies() []C.Proxy {
	return u.GetProxies(false)
}

func (u *URLTest) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	delays, err := u.GroupBase.URLTest(ctx, u.testUrl, expectedStatus)
	// URL tests update alive/delay history; reset cache so next routing picks fresh best node.
	u.fastSingle.Reset()
	_ = u.fastWithHealthCheck(false, false)
	return delays, err
}

func NewURLTest(option GroupCommonOption, urlTestOption URLTestOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*URLTest, error) {
	if emptyFallback == nil {
		return nil, errors.New("empty fallback proxy not exist")
	}
	groupBase, err := NewGroupBase(GroupBaseOption{
		Name:           option.Name,
		Type:           C.URLTest,
		Hidden:         option.Hidden,
		Icon:           option.Icon,
		Filter:         option.Filter,
		ExcludeFilter:  option.ExcludeFilter,
		ExcludeType:    option.ExcludeType,
		TestTimeout:    option.TestTimeout,
		MaxFailedTimes: option.MaxFailedTimes,
		EmptyFallback:  emptyFallback,
		Providers:      providers,
	})
	if err != nil {
		return nil, err
	}

	urlTest := &URLTest{
		GroupBase:      groupBase,
		fastSingle:     singledo.NewSingle[C.Proxy](time.Second * 10),
		disableUDP:     option.DisableUDP,
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
		tolerance:      urlTestOption.Tolerance,
	}

	return urlTest, nil
}
