package provider

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/singledo"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/dlclark/regexp2"
	"golang.org/x/sync/errgroup"
)

type HealthCheckOption struct {
	URL      string
	Interval uint
}

type extraOption struct {
	expectedStatus utils.IntRanges[uint16]
	filters        []extraFilter
}

type extraFilter struct {
	filters        map[string]struct{}
	excludeFilters map[string]struct{}
	excludeTypes   []string
}

type compiledExtraFilter struct {
	filterReg        *regexp2.Regexp
	excludeFilterReg *regexp2.Regexp
	excludeTypes     []string
}

type HealthCheck struct {
	ctx            context.Context
	ctxCancel      context.CancelFunc
	url            string
	extra          map[string]*extraOption
	mu             sync.Mutex
	proxies        []C.Proxy
	interval       time.Duration
	lazy           bool
	expectedStatus utils.IntRanges[uint16]
	lastTouch      atomic.TypedValue[time.Time]
	singleDo       *singledo.Single[struct{}]
	timeout        time.Duration
}

func (hc *HealthCheck) process() {
	ticker := time.NewTicker(hc.interval)
	go hc.check()
	for {
		select {
		case <-ticker.C:
			lastTouch := hc.lastTouch.Load()
			since := time.Since(lastTouch)
			if !hc.lazy || since < hc.interval {
				hc.check()
			} else {
				log.Debugln("Skip once health check because we are lazy")
			}
		case <-hc.ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (hc *HealthCheck) setProxies(proxies []C.Proxy) {
	hc.proxies = proxies
}

func (hc *HealthCheck) registerHealthCheckTask(url string, expectedStatus utils.IntRanges[uint16], filter string, excludeFilter string, excludeType string, interval uint) {
	url = strings.TrimSpace(url)
	if len(url) == 0 || url == hc.url {
		log.Debugln("ignore invalid health check url: %s", url)
		return
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()

	// if the provider has not set up health checks, then modify it to be the same as the group's interval
	if hc.interval == 0 {
		hc.interval = time.Duration(interval) * time.Second
	}

	if hc.extra == nil {
		hc.extra = make(map[string]*extraOption)
	}

	// prioritize the use of previously registered configurations, especially those from provider
	if _, ok := hc.extra[url]; ok {
		hc.extra[url].filters = append(hc.extra[url].filters, newExtraFilter(filter, excludeFilter, excludeType))

		log.Debugln("health check url: %s exists", url)
		return
	}

	option := &extraOption{
		expectedStatus: expectedStatus,
		filters:        []extraFilter{newExtraFilter(filter, excludeFilter, excludeType)},
	}
	hc.extra[url] = option
}

func newExtraFilter(filter string, excludeFilter string, excludeType string) extraFilter {
	option := extraFilter{filters: map[string]struct{}{}, excludeFilters: map[string]struct{}{}}
	splitAndAddFiltersToExtra(filter, option.filters)
	splitAndAddFiltersToExtra(excludeFilter, option.excludeFilters)
	addExcludeTypesToExtra(excludeType, &option)
	return option
}

func splitAndAddFiltersToExtra(filter string, filters map[string]struct{}) {
	filter = strings.TrimSpace(filter)
	if len(filter) == 0 || filters == nil {
		return
	}

	for _, regex := range strings.Split(filter, "`") {
		regex = strings.TrimSpace(regex)
		if len(regex) != 0 {
			filters[regex] = struct{}{}
		}
	}
}

func addExcludeTypesToExtra(excludeType string, option *extraFilter) {
	excludeType = strings.TrimSpace(excludeType)
	if len(excludeType) == 0 || option == nil {
		return
	}

	exists := map[string]struct{}{}
	for _, typ := range option.excludeTypes {
		exists[strings.ToLower(typ)] = struct{}{}
	}
	for _, typ := range strings.Split(excludeType, "|") {
		typ = strings.TrimSpace(typ)
		if typ == "" {
			continue
		}
		key := strings.ToLower(typ)
		if _, ok := exists[key]; ok {
			continue
		}
		exists[key] = struct{}{}
		option.excludeTypes = append(option.excludeTypes, typ)
	}
}

func compileExtraFilters(filters []extraFilter) []compiledExtraFilter {
	compiled := make([]compiledExtraFilter, 0, len(filters))
	for _, filter := range filters {
		compiled = append(compiled, compiledExtraFilter{
			filterReg:        compileHealthCheckFilter(filter.filters),
			excludeFilterReg: compileHealthCheckFilter(filter.excludeFilters),
			excludeTypes:     filter.excludeTypes,
		})
	}
	return compiled
}

func compileHealthCheckFilter(filters map[string]struct{}) *regexp2.Regexp {
	if len(filters) == 0 {
		return nil
	}

	expressions := make([]string, 0, len(filters))
	for filter := range filters {
		expressions = append(expressions, filter)
	}
	return regexp2.MustCompile(strings.Join(expressions, "|"), regexp2.None)
}

func (filter compiledExtraFilter) match(proxy C.Proxy) bool {
	if filter.filterReg != nil {
		if match, _ := filter.filterReg.MatchString(proxy.Name()); !match {
			return false
		}
	}
	if filter.excludeFilterReg != nil {
		if match, _ := filter.excludeFilterReg.MatchString(proxy.Name()); match {
			return false
		}
	}
	for _, excludeType := range filter.excludeTypes {
		if strings.EqualFold(proxy.Type().String(), excludeType) {
			return false
		}
	}
	return true
}

func (hc *HealthCheck) auto() bool {
	return hc.interval != 0
}

func (hc *HealthCheck) touch() {
	hc.lastTouch.Store(time.Now())
}

func (hc *HealthCheck) check() {
	if len(hc.proxies) == 0 {
		return
	}

	_, _, _ = hc.singleDo.Do(func() (struct{}, error) {
		id := utils.NewUUIDV4().String()
		log.Debugln("Start New Health Checking {%s}", id)
		b := new(errgroup.Group)
		b.SetLimit(20)

		// execute default health check
		option := &extraOption{filters: nil, expectedStatus: hc.expectedStatus}
		hc.execute(b, hc.url, id, option)

		// execute extra health check
		if len(hc.extra) != 0 {
			for url, option := range hc.extra {
				hc.execute(b, url, id, option)
			}
		}
		_ = b.Wait()
		log.Debugln("Finish A Health Checking {%s}", id)
		return struct{}{}, nil
	})
}

func (hc *HealthCheck) execute(b *errgroup.Group, url, uid string, option *extraOption) {
	url = strings.TrimSpace(url)
	if len(url) == 0 {
		log.Debugln("Health Check has been skipped due to testUrl is empty, {%s}", uid)
		return
	}

	var filters []compiledExtraFilter
	var expectedStatus utils.IntRanges[uint16]
	if option != nil {
		expectedStatus = option.expectedStatus
		filters = compileExtraFilters(option.filters)
	}

	for _, proxy := range hc.proxies {
		if len(filters) != 0 {
			matched := false
			for _, filter := range filters {
				if filter.match(proxy) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}

		p := proxy
		b.Go(func() error {
			ctx, cancel := context.WithTimeout(hc.ctx, hc.timeout)
			defer cancel()
			log.Debugln("Health Checking, proxy: %s, url: %s, id: {%s}", p.Name(), url, uid)
			_, _ = p.URLTest(ctx, url, expectedStatus)
			log.Debugln("Health Checked, proxy: %s, url: %s, alive: %t, delay: %d ms uid: {%s}", p.Name(), url, p.AliveForTestUrl(url), p.LastDelayForTestUrl(url), uid)
			return nil
		})
	}
}

func (hc *HealthCheck) close() {
	hc.ctxCancel()
}

func NewHealthCheck(proxies []C.Proxy, url string, timeout uint, interval uint, lazy bool, expectedStatus utils.IntRanges[uint16]) *HealthCheck {
	if url == "" {
		expectedStatus = nil
		interval = 0
	}
	if timeout == 0 {
		timeout = 5000
	}
	ctx, cancel := context.WithCancel(context.Background())

	return &HealthCheck{
		ctx:            ctx,
		ctxCancel:      cancel,
		proxies:        proxies,
		url:            url,
		timeout:        time.Duration(timeout) * time.Millisecond,
		extra:          map[string]*extraOption{},
		interval:       time.Duration(interval) * time.Second,
		lazy:           lazy,
		expectedStatus: expectedStatus,
		singleDo:       singledo.NewSingle[struct{}](time.Duration(timeout) * time.Millisecond), // keep in sync with test timeout
	}
}
