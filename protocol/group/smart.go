package group

import (
	"context"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/ring"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	tun "github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/contrab/freelru"
	"github.com/sagernet/sing/contrab/maphash"
	"github.com/sagernet/sing/service"
)

const (
	smartSaveInterval = 5 * time.Second

	// TODO(mmotyshen): Move to config.
	// TODO(mmotyshen): Maybe, separate size of `selected` and `learning`.
	smartLearningCacheSize = 10000 // Hardcoded LRU size for domain learning cache.
)

func RegisterSmart(registry *outbound.Registry) {
	outbound.Register[option.SmartOutboundOptions](registry, C.TypeSmart, NewSmart)
}

var (
	_ adapter.OutboundGroup             = (*Smart)(nil)
	_ adapter.ConnectionHandlerEx       = (*Smart)(nil)
	_ adapter.PacketConnectionHandlerEx = (*Smart)(nil)
)

type Smart struct {
	outbound.Adapter
	ctx        context.Context
	outbound   adapter.OutboundManager
	connection adapter.ConnectionManager
	logger     log.ContextLogger
	tags       []string

	requiredSuccessRate        float64
	recheckInterval            time.Duration
	requestSampleCount         int
	requiredRequestSampleCount int

	group *SmartGroup
}

func NewSmart(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.SmartOutboundOptions) (adapter.Outbound, error) {
	s := &Smart{
		Adapter:                    outbound.NewAdapter(C.TypeSmart, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		ctx:                        ctx,
		outbound:                   service.FromContext[adapter.OutboundManager](ctx),
		connection:                 service.FromContext[adapter.ConnectionManager](ctx),
		logger:                     logger,
		tags:                       options.Outbounds,
		requiredSuccessRate:        options.RequiredSuccessRate,
		recheckInterval:            options.RecheckInterval.Build(),
		requestSampleCount:         options.RequestSampleCount,
		requiredRequestSampleCount: options.RequiredRequestSampleCount,
	}
	if len(s.tags) == 0 {
		return nil, E.New("missing tags")
	}
	return s, nil
}

func (s *Smart) Start() error {
	outbounds := make([]adapter.Outbound, 0, len(s.tags))
	for i, tag := range s.tags {
		detour, loaded := s.outbound.Outbound(tag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", tag)
		}
		outbounds = append(outbounds, detour)
	}
	group, err := NewSmartGroup(
		s.ctx,
		s.outbound,
		s.logger,
		s.Tag(),
		outbounds,
		s.requiredSuccessRate,
		s.recheckInterval,
		s.requestSampleCount,
		s.requiredRequestSampleCount,
	)
	if err != nil {
		return err
	}
	s.group = group
	return nil
}

func (s *Smart) Close() error {
	return common.Close(common.PtrOrNil(s.group))
}

func (s *Smart) Now() string {
	return s.group.outbounds[0].Tag()
}
func (s *Smart) All() []string {
	return s.tags
}

func (s *Smart) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	var outbound adapter.Outbound
	switch N.NetworkName(network) {
	case N.NetworkTCP, N.NetworkUDP:
		outbound, _ = s.group.Select(network, destination)
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := outbound.DialContext(ctx, network, destination)
	if err != nil {
		s.logger.ErrorContext(ctx, err)
		s.group.Observe(destination, outbound, false, 0)
		return nil, err
	}
	conn = newConnObserver(s.group, destination.Fqdn, outbound.Tag(), conn)
	return s.group.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}

// TODO(mmotyshen): this method should probably also return a connection wrapped in observer.
func (s *Smart) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	outbound, _ := s.group.Select(N.NetworkUDP, destination)
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := outbound.ListenPacket(ctx, destination)
	if err == nil {
		s.group.Observe(destination, outbound, true, 0)
		return s.group.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	s.group.Observe(destination, outbound, false, 0)
	return nil, err
}

func (s *Smart) NewConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	s.connection.NewConnection(ctx, s, conn, metadata, onClose)
}

func (s *Smart) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	s.connection.NewPacketConnection(ctx, s, conn, metadata, onClose)
}

func (s *Smart) NewDirectRouteConnection(metadata adapter.InboundContext, routeContext tun.DirectRouteContext, timeout time.Duration) (tun.DirectRouteDestination, error) {
	var selected adapter.Outbound
	sockAddr := M.ParseSocksaddr(metadata.Domain)
	if metadata.Domain != "" {
		selected, _ = s.group.Select(N.NetworkTCP, sockAddr)
	}
	if selected == nil {
		selected = s.group.outbounds[0]
	}
	if selected == nil {
		return nil, E.New("missing supported outbound")
	}
	if !common.Contains(selected.Network(), metadata.Network) {
		return nil, E.New(metadata.Network, " is not supported by outbound: ", selected.Tag())
	}
	return selected.(adapter.DirectRouteOutbound).NewDirectRouteConnection(metadata, routeContext, timeout)
}

type SelectedOutbound struct {
	Outbound   adapter.Outbound
	ValidUntil time.Time
}

type SmartGroup struct {
	tag            string
	outbounds      []adapter.Outbound
	cacheFile      adapter.CacheFile
	logger         log.Logger
	learningAccess sync.RWMutex
	learning       freelru.Cache[string, *SmartDomainStats]
	selectedAccess sync.RWMutex
	selected       map[string]SelectedOutbound // TODO(mmotyshen): How to limit the size? Maybe, just evict when `learning.Add` returns true.
	saveAccess     sync.Mutex
	saveTimer      *time.Timer
	interruptGroup *interrupt.Group

	requestSampleCount         int
	requiredRequestSampleCount int
	requiredSuccessRate        float64 // TODO(mmotyshen): change to absolute count and validate it.
	recheckInterval            time.Duration
}

func NewSmartGroup(
	ctx context.Context,
	outboundManager adapter.OutboundManager,
	logger log.Logger,
	tag string,
	outbounds []adapter.Outbound,
	requiredSuccessRate float64,
	recheckInterval time.Duration,
	requestSampleCount int,
	requiredRequestSampleCount int,
) (*SmartGroup, error) {
	if requiredSuccessRate <= 0 || requiredSuccessRate > 1 {
		requiredSuccessRate = 0.5
	}
	if recheckInterval <= 0 {
		recheckInterval = 5 * time.Minute
	}
	if requestSampleCount <= 0 {
		requestSampleCount = 10
	}
	if requiredRequestSampleCount <= 0 || requiredRequestSampleCount > requestSampleCount {
		requiredRequestSampleCount = 3
	}
	learningCache, err := freelru.NewSharded[string, *SmartDomainStats]( // TODO(mmotyshen): maybe, not sharded?
		smartLearningCacheSize,
		maphash.NewHasher[string]().Hash32,
	)
	if err != nil {
		return nil, E.Cause(err, "create learning cache")
	}
	group := &SmartGroup{
		tag:                        tag,
		outbounds:                  outbounds,
		cacheFile:                  service.FromContext[adapter.CacheFile](ctx),
		logger:                     logger,
		learning:                   learningCache,
		selected:                   make(map[string]SelectedOutbound),
		interruptGroup:             interrupt.NewGroup(),
		requestSampleCount:         requestSampleCount,
		requiredRequestSampleCount: requiredRequestSampleCount,
		requiredSuccessRate:        requiredSuccessRate,
		recheckInterval:            recheckInterval,
	}
	if group.cacheFile != nil {
		if state := group.cacheFile.LoadSmartRouting(tag); state != nil && len(state.StatsByDomains) > 0 {
			for _, cacheDomainStats := range state.StatsByDomains {
				if cacheDomainStats.Domain == "" {
					continue // TODO(mmotyshen): log.
				}
				outbounds := make(map[string]SmartOutboundStats, len(cacheDomainStats.Outbounds))
				for tag, stats := range cacheDomainStats.Outbounds {
					ringBuffer := ring.New[SmartRequestResult](requestSampleCount)
					for _, rr := range stats.RequestResults {
						ringBuffer.Push(SmartRequestResult{
							Success: rr.Success,
							Delay:   rr.Delay,
						})
					}
					outbounds[tag] = SmartOutboundStats{
						RequestResults:  ringBuffer,
						LastRecheckTime: stats.LastRecheckTime,
					}
				}
				group.learning.Add(cacheDomainStats.Domain, &SmartDomainStats{
					Outbounds: outbounds,
				})
			}
		}
	}
	return group, nil
}

func (g *SmartGroup) Close() error {
	g.saveAccess.Lock()
	if g.saveTimer != nil {
		g.saveTimer.Stop()
		g.saveTimer = nil
	}
	g.saveAccess.Unlock()
	g.save()
	return nil
}

// Observe records a single request result for the (domain, outbound) pair.
func (g *SmartGroup) Observe(destination M.Socksaddr, detour adapter.Outbound, success bool, delay time.Duration) {
	g.observe(destination.Fqdn, RealTag(detour), success, delay)
}

func (g *SmartGroup) observe(domain, tag string, success bool, delay time.Duration) {
	if domain == "" {
		return
	}
	g.learningAccess.Lock()
	domainState, ok := g.learning.Get(domain)
	if !ok || domainState == nil {
		domainState = &SmartDomainStats{
			Outbounds: make(map[string]SmartOutboundStats),
		}
	}
	if domainState.Outbounds == nil {
		domainState.Outbounds = make(map[string]SmartOutboundStats)
	}
	stats := domainState.Outbounds[tag]
	if stats.RequestResults == nil {
		stats.RequestResults = ring.New[SmartRequestResult](g.requestSampleCount)
	}
	result := SmartRequestResult{Success: success, Delay: delay}
	stats.RequestResults.Push(result)
	domainState.Outbounds[tag] = stats
	g.learning.Add(domain, domainState)
	g.learningAccess.Unlock()
	g.logger.Debug("smart observe: domain=", domain, " outbound=", tag, " success=", success, " delay=", delay)
	g.scheduleSave()
}

func (g *SmartGroup) Select(network string, destination M.Socksaddr) (adapter.Outbound, bool) {
	domain := destination.Fqdn
	if domain == "" {
		return g.outbounds[0], false
	}
	// TODO(mmotyshen): the `selected` field should only be used if the recheck period has not yet passed.
	g.selectedAccess.RLock()
	sel, ok := g.selected[domain]
	g.selectedAccess.RUnlock()
	now := time.Now()
	// TODO(mmotyshen): Maybe, the domain stats must also distinguish between types of networks.
	if ok && sel.ValidUntil.After(now) && common.Contains(sel.Outbound.Network(), network) {
		return sel.Outbound, true
	}
	o, isBest, bestCheckedAt := g.selectOutbound(domain, network)
	if isBest {
		g.setSelected(domain, o, bestCheckedAt)
		return o, true
	}
	return o, false
}

func (g *SmartGroup) selectOutbound(
	domain, network string,
) (outbound adapter.Outbound, isBest bool, bestCheckedAt time.Time) {
	g.learningAccess.RLock()
	domainStats, ok := g.learning.Get(domain)
	g.learningAccess.RUnlock()
	if !ok || domainStats == nil {
		return g.selectRandomOutbound(network), false, time.Time{}
	}
	var (
		best         adapter.Outbound
		bestDelayAvg float64
		now          = time.Now()
	)
	for _, i := range rand.Perm(len(g.outbounds)) {
		outbound := g.outbounds[i]
		if !slices.Contains(outbound.Network(), network) {
			continue
		}
		outboundStats, ok := domainStats.Outbounds[outbound.Tag()]
		if !ok {
			return outbound, false, time.Time{}
		}
		if now.Sub(outboundStats.LastRecheckTime) > g.recheckInterval {
			return outbound, false, time.Time{}
		}
		if outboundStats.RequestResults.Size() < g.requiredRequestSampleCount {
			return outbound, false, time.Time{}
		}
		var (
			successCount int
			delaySum     time.Duration
		)
		for _, r := range outboundStats.RequestResults.Slice() {
			if r.Success {
				successCount++
				delaySum += r.Delay
			}
		}
		successRate := float64(successCount) / float64(outboundStats.RequestResults.Size())
		if successRate < g.requiredSuccessRate {
			continue
		}
		delayAvg := float64(delaySum) / float64(successCount)
		if best == nil || delayAvg < bestDelayAvg {
			best = outbound
			bestDelayAvg = delayAvg
			bestCheckedAt = outboundStats.LastRecheckTime
		}
	}
	if best == nil {
		return g.selectRandomOutbound(network), false, time.Time{}
	}
	return best, true, bestCheckedAt
}

func (g *SmartGroup) selectRandomOutbound(network string) adapter.Outbound {
	for _, i := range rand.Perm(len(g.outbounds)) {
		outbound := g.outbounds[i]
		if slices.Contains(outbound.Network(), network) {
			return outbound
		}
	}
	return nil
}

// TODO(mmotyshen): not used.
func (g *SmartGroup) maybeExplore(domain, network string, currentBest adapter.Outbound) adapter.Outbound {
	if g.recheckInterval <= 0 || currentBest == nil {
		return currentBest
	}
	now := time.Now()
	g.learningAccess.RLock()
	ds, ok := g.learning.Get(domain)
	g.learningAccess.RUnlock()
	if !ok || ds == nil {
		return currentBest
	}
	for _, detour := range g.outbounds {
		if !common.Contains(detour.Network(), network) || detour == currentBest {
			continue
		}
		st := ds.Outbounds[RealTag(detour)]
		if now.Sub(st.LastRecheckTime) > g.recheckInterval {
			return detour
		}
	}
	return currentBest
}

func (g *SmartGroup) setSelected(
	domain string,
	outbound adapter.Outbound,
	checkedAt time.Time,
) {
	if outbound == nil || domain == "" {
		return
	}
	tag := RealTag(outbound)
	g.updateRecheckTime(domain, tag)
	g.selectedAccess.Lock()
	g.selected[domain] = SelectedOutbound{
		Outbound:   outbound,
		ValidUntil: checkedAt.Add(g.recheckInterval),
	}
	g.selectedAccess.Unlock()
}

// TODO(mmotyshen): Probably, must delete this.
func (g *SmartGroup) updateRecheckTime(domain, tag string) {
	now := time.Now()
	g.learningAccess.Lock()
	ds, ok := g.learning.Get(domain)
	if !ok || ds == nil {
		ds = &SmartDomainStats{
			Outbounds: make(map[string]SmartOutboundStats),
		}
	}
	if ds.Outbounds == nil {
		ds.Outbounds = make(map[string]SmartOutboundStats)
	}
	st := ds.Outbounds[tag]
	st.LastRecheckTime = now
	ds.Outbounds[tag] = st
	g.learning.Add(domain, ds)
	g.learningAccess.Unlock()
}

// TODO(mmotyshen): Probably, must delete this.
// func (g *SmartGroup) triggerSelectionUpdate(domain, network string) {
// 	if domain == "" {
// 		return
// 	}
// 	go func() { // TODO(mmotyshen): allow a single goroutine per domain?
// 		best := g.selectBest(domain, network)
// 		if best == nil {
// 			return
// 		}
// 		g.selectedAccess.Lock()
// 		current := g.selected[domain]
// 		changed := current != best
// 		if changed {
// 			g.selected[domain] = best
// 		}
// 		g.selectedAccess.Unlock()
// 		if changed {
// 			g.updateRecheckTime(domain, RealTag(best))
// 			g.logger.Debug("smart updated selection: domain=", domain, " outbound=", best.Tag())
// 		}
// 	}()
// }

func (g *SmartGroup) scheduleSave() {
	g.saveAccess.Lock()
	defer g.saveAccess.Unlock()
	if g.cacheFile == nil {
		return
	}
	if g.saveTimer == nil {
		g.saveTimer = time.AfterFunc(smartSaveInterval, func() {
			g.saveAccess.Lock()
			g.saveTimer = nil
			g.saveAccess.Unlock()
			g.save()
		})
	} else {
		g.saveTimer.Reset(smartSaveInterval)
	}
}

func (g *SmartGroup) save() {
	cacheFile := g.cacheFile
	if cacheFile == nil {
		return
	}
	g.learningAccess.RLock()
	cacheState := &adapter.SmartRoutingStats{
		StatsByDomains: make([]adapter.SmartDomainStats, g.learning.Len()),
	}
	for _, domain := range g.learning.Keys() {
		if domainStats, ok := g.learning.Peek(domain); ok && domainStats != nil {
			cacheDomainState := adapter.SmartDomainStats{
				Domain:    domain,
				Outbounds: make(map[string]adapter.SmartOutboundStats, len(domainStats.Outbounds)),
			}
			for outbound, outboundStats := range domainStats.Outbounds {
				resultsToStore := make([]adapter.SmartRequestResult, 0, outboundStats.RequestResults.Size())
				for _, rr := range outboundStats.RequestResults.Slice() {
					resultsToStore = append(resultsToStore, adapter.SmartRequestResult{
						Success: rr.Success,
						Delay:   rr.Delay,
					})
				}
				cacheDomainState.Outbounds[outbound] = adapter.SmartOutboundStats{
					RequestResults:  resultsToStore,
					LastRecheckTime: outboundStats.LastRecheckTime,
				}
			}
			cacheState.StatsByDomains = append(cacheState.StatsByDomains, cacheDomainState)
		}
	}
	g.learningAccess.RUnlock()
	if err := cacheFile.StoreSmartRouting(g.tag, cacheState); err != nil {
		g.logger.Warn("save smart routing: ", err)
	} else {
		g.logger.Debug("smart saved domains: ", len(cacheState.StatsByDomains))
	}
}

type SmartRoutingStats struct {
	StatsByDomains freelru.ShardedLRU[string, *SmartDomainStats]
}

type SmartDomainStats struct {
	Outbounds map[string]SmartOutboundStats
}

type SmartOutboundStats struct {
	RequestResults  *ring.Buffer[SmartRequestResult]
	LastRecheckTime time.Time // FIXME(mmotyshen): not being set anywhere!
}

type SmartRequestResult struct {
	Success bool
	Delay   time.Duration
}
