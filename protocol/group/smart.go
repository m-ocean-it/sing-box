package group

import (
	"context"
	"maps"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	tun "github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

const (
	smartSaveInterval = 5 * time.Second
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

func (s *Smart) PostStart() error {
	s.group.URLTestGroup.PostStart()
	return nil
}

// Now returns the outbound currently selected by the embedded URLTest group.
func (s *Smart) Now() string {
	return s.group.URLTestGroup.Now()
}

// TODO(mmotyshen): Is this method needed?
func (s *Smart) All() []string {
	return s.tags
}

func (s *Smart) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	s.group.Touch()
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
	domain := destination.Fqdn
	s.group.triggerSelectionUpdate(domain, network)
	conn = newConnObserver(s.group, domain, outbound.Tag(), conn)
	return s.group.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}

func (s *Smart) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	s.group.Touch()
	outbound, _ := s.group.Select(N.NetworkUDP, destination)
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := outbound.ListenPacket(ctx, destination)
	domain := destination.Fqdn
	if err == nil {
		s.group.Observe(destination, outbound, true, 0)
		if domain != "" {
			s.group.triggerSelectionUpdate(domain, N.NetworkUDP)
		}
		return s.group.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	s.group.Observe(destination, outbound, false, 0)
	if domain != "" {
		s.group.triggerSelectionUpdate(domain, N.NetworkUDP)
	}
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
	if s.group != nil {
		s.group.Touch()
	}
	var selected adapter.Outbound
	sockAddr := M.ParseSocksaddr(metadata.Domain)
	if metadata.Domain != "" {
		selected, _ = s.group.Select(N.NetworkTCP, sockAddr)
	}
	if selected == nil {
		selected, _ = s.group.URLTestGroup.Select(N.NetworkTCP)
	}
	if selected == nil {
		return nil, E.New("missing supported outbound")
	}
	if !common.Contains(selected.Network(), metadata.Network) {
		return nil, E.New(metadata.Network, " is not supported by outbound: ", selected.Tag())
	}
	return selected.(adapter.DirectRouteOutbound).NewDirectRouteConnection(metadata, routeContext, timeout)
}

type SmartGroup struct {
	tag       string
	outbounds []adapter.Outbound
	cacheFile adapter.CacheFile
	// TODO(mmotyshen): URLTest fallback is meaningless, because, instead of falling back,
	// the system must learn until it has enough data to intelligently select outbounds.
	*URLTestGroup
	logger         log.Logger
	learningAccess sync.RWMutex
	learning       map[string]adapter.SmartDomainStats
	selectedAccess sync.RWMutex
	selected       map[string]adapter.Outbound
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
	urlTestGroup, err := NewURLTestGroup(ctx, outboundManager, logger, outbounds, "", 0, 0, 0, false)
	if err != nil {
		return nil, err
	}
	group := &SmartGroup{
		tag:                        tag,
		outbounds:                  outbounds,
		cacheFile:                  service.FromContext[adapter.CacheFile](ctx),
		logger:                     logger,
		URLTestGroup:               urlTestGroup,
		learning:                   make(map[string]adapter.SmartDomainStats),
		selected:                   make(map[string]adapter.Outbound),
		interruptGroup:             interrupt.NewGroup(),
		requestSampleCount:         requestSampleCount,
		requiredRequestSampleCount: requiredRequestSampleCount,
		requiredSuccessRate:        requiredSuccessRate,
		recheckInterval:            recheckInterval,
	}
	if group.cacheFile != nil {
		if state := group.cacheFile.LoadSmartRouting(tag); state != nil && len(state.StatsByDomains) > 0 {
			group.learning = make(map[string]adapter.SmartDomainStats, len(state.StatsByDomains))
			for d, ds := range state.StatsByDomains {
				copyDs := adapter.SmartDomainStats{
					Outbounds: make(map[string]adapter.SmartOutboundStats, len(ds.Outbounds)),
				}
				maps.Copy(copyDs.Outbounds, ds.Outbounds)
				group.learning[d] = copyDs
			}
		}
	}
	return group, nil
}

func (g *SmartGroup) Touch() {
	// TODO(mmotyshen): Is this method needed?
	// TODO(mmotyshen): Maybe, need to touch the embedded urltest group?
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
	domainState := g.learning[domain]
	if domainState.Outbounds == nil {
		domainState.Outbounds = make(map[string]adapter.SmartOutboundStats)
	}
	stats := domainState.Outbounds[tag]
	maxSamples := max(g.requestSampleCount, 1)
	result := adapter.SmartRequestResult{Success: success, Delay: delay}
	// TODO(mmotyshen): extract ring buffer implementation to own package, add tests, then use it here.
	if len(stats.LastRequestResultsRing) < maxSamples {
		stats.LastRequestResultsRing = append(stats.LastRequestResultsRing, result)
		stats.LastRequestResultsRingCursor = len(stats.LastRequestResultsRing)
	} else {
		cursor := stats.LastRequestResultsRingCursor % g.requestSampleCount
		stats.LastRequestResultsRing[cursor] = result
		stats.LastRequestResultsRingCursor = (cursor + 1) % g.requestSampleCount
	}
	domainState.Outbounds[tag] = stats
	g.learning[domain] = domainState
	g.learningAccess.Unlock()
	g.logger.Debug("smart observe: domain=", domain, " outbound=", tag, " success=", success, " delay=", delay)
	g.scheduleSave()
}

func (g *SmartGroup) Select(network string, destination M.Socksaddr) (adapter.Outbound, bool) {
	domain := destination.Fqdn
	if domain == "" {
		return g.URLTestGroup.Select(network)
	}
	// TODO(mmotyshen): the `selected` field should only be used if the recheck period has not yet passed.
	g.selectedAccess.RLock()
	sel := g.selected[domain]
	g.selectedAccess.RUnlock()
	// TODO(mmotyshen): Maybe, the domain stats must also distinguish between types of networks.
	if sel != nil && common.Contains(sel.Network(), network) {
		return sel, true
	}
	best := g.selectBest(domain, network)
	if best != nil {
		g.setSelected(domain, best)
		return best, true
	}
	// TODO(mmotyshen): Maybe, select the outbound with the least stats instead of randomly.
	for _, i := range rand.Perm(len(g.outbounds)) {
		o := g.outbounds[i]
		if common.Contains(o.Network(), network) {
			return o, false // TODO(mmotyshen): Should it really be `false`, not `true`?
		}
	}
	return nil, false
}

func (g *SmartGroup) selectBest(domain, network string) adapter.Outbound {
	g.learningAccess.RLock()
	domainStats, ok := g.learning[domain]
	g.learningAccess.RUnlock()
	if !ok {
		return nil
	}
	var (
		best         adapter.Outbound
		bestDelayAvg float64
	)
	for _, detour := range g.outbounds {
		if !slices.Contains(detour.Network(), network) {
			continue
		}
		outboundStats, ok := domainStats.Outbounds[detour.Tag()]
		if !ok {
			// TODO(mmotyshen): Make sure to, at some point, collect stats for a new outbound.
			continue
		}
		if len(outboundStats.LastRequestResultsRing) < g.requiredRequestSampleCount {
			continue // Not enough requests collected.
		}
		var (
			successCount int
			delaySum     time.Duration
		)
		for _, r := range outboundStats.LastRequestResultsRing {
			if r.Success {
				successCount++
				delaySum += r.Delay
			}
		}
		successRate := float64(successCount) / float64(len(outboundStats.LastRequestResultsRing))
		if successRate < g.requiredSuccessRate {
			continue // Not enough successful requests collected.
		}
		delayAvg := float64(delaySum) / float64(successCount)
		if best == nil || delayAvg < bestDelayAvg {
			best = detour
			bestDelayAvg = delayAvg
		}
	}

	return best // nil is allowed.
}

// TODO(mmotyshen): not used.
func (g *SmartGroup) maybeExplore(domain, network string, currentBest adapter.Outbound) adapter.Outbound {
	if g.recheckInterval <= 0 || currentBest == nil {
		return currentBest
	}
	now := time.Now()
	g.learningAccess.RLock()
	ds := g.learning[domain]
	for _, detour := range g.outbounds {
		if !common.Contains(detour.Network(), network) || detour == currentBest {
			continue
		}
		st := ds.Outbounds[RealTag(detour)]
		if now.Sub(st.LastRecheckTime) > g.recheckInterval {
			g.learningAccess.RUnlock()
			return detour
		}
	}
	g.learningAccess.RUnlock()
	return currentBest
}

func (g *SmartGroup) setSelected(domain string, out adapter.Outbound) {
	if out == nil || domain == "" {
		return
	}
	tag := RealTag(out)
	// TODO(mmotyshen): Recheck time implies that actual metrics were obtained, but current implementation
	// calls setSelected as a fallback even without any data.
	g.updateRecheckTime(domain, tag)
	g.selectedAccess.Lock()
	g.selected[domain] = out
	g.selectedAccess.Unlock()
}

func (g *SmartGroup) updateRecheckTime(domain, tag string) {
	now := time.Now()
	g.learningAccess.Lock()
	ds := g.learning[domain]
	if ds.Outbounds == nil {
		ds.Outbounds = make(map[string]adapter.SmartOutboundStats)
	}
	st := ds.Outbounds[tag]
	st.LastRecheckTime = now
	ds.Outbounds[tag] = st
	g.learning[domain] = ds
	g.learningAccess.Unlock()
}

// TODO(mmotyshen): This method should not actually do anything
// unless recheck period has passed since last recheck.
func (g *SmartGroup) triggerSelectionUpdate(domain, network string) {
	if domain == "" {
		return
	}
	go func() { // TODO(mmotyshen): allow a single goroutine per domain?
		best := g.selectBest(domain, network)
		if best == nil {
			return
		}
		g.selectedAccess.Lock()
		current := g.selected[domain]
		changed := current != best
		if changed {
			g.selected[domain] = best
		}
		g.selectedAccess.Unlock()
		if changed {
			g.updateRecheckTime(domain, RealTag(best))
			g.logger.Debug("smart updated selection: domain=", domain, " outbound=", best.Tag())
		}
	}()
}

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
	state := &adapter.SmartRoutingStats{
		StatsByDomains: make(map[string]adapter.SmartDomainStats, len(g.learning)),
	}
	for domain, domainStats := range g.learning {
		copyState := adapter.SmartDomainStats{
			Outbounds: make(map[string]adapter.SmartOutboundStats, len(domainStats.Outbounds)),
		}
		maps.Copy(copyState.Outbounds, domainStats.Outbounds)
		state.StatsByDomains[domain] = copyState
	}
	g.learningAccess.RUnlock()
	if err := cacheFile.StoreSmartRouting(g.tag, state); err != nil {
		g.logger.Warn("save smart routing: ", err)
	} else {
		g.logger.Debug("smart saved domains: ", len(state.StatsByDomains))
	}
}
