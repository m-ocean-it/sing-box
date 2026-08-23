package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/observable"
	"github.com/sagernet/sing/common/varbin"
)

type ClashServer interface {
	LifecycleService
	ConnectionTracker
	Mode() string
	ModeList() []string
	SetModeUpdateHook(hook *observable.Subscriber[struct{}])
	HistoryStorage() URLTestHistoryStorage
}

type URLTestHistory struct {
	Time  time.Time `json:"time"`
	Delay uint16    `json:"delay"`
}

type URLTestHistoryStorage interface {
	SetHook(hook *observable.Subscriber[struct{}])
	LoadURLTestHistory(tag string) *URLTestHistory
	DeleteURLTestHistory(tag string)
	StoreURLTestHistory(tag string, history *URLTestHistory)
	Close() error
}

type V2RayServer interface {
	LifecycleService
	StatsService() ConnectionTracker
}

type CacheFile interface {
	LifecycleService

	StoreFakeIP() bool
	FakeIPStorage

	StoreRDRC() bool
	RDRCStore

	LoadMode() string
	StoreMode(mode string) error
	LoadSelected(group string) string
	StoreSelected(group string, selected string) error
	LoadGroupExpand(group string) (isExpand bool, loaded bool)
	StoreGroupExpand(group string, expand bool) error
	LoadRuleSet(tag string) *SavedBinary
	SaveRuleSet(tag string, set *SavedBinary) error
	LoadSmartRouting(tag string) *SmartRoutingStats
	StoreSmartRouting(tag string, state *SmartRoutingStats) error
}

type SavedBinary struct {
	Content     []byte
	LastUpdated time.Time
	LastEtag    string
}

func (s *SavedBinary) MarshalBinary() ([]byte, error) {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, uint8(1))
	if err != nil {
		return nil, err
	}
	_, err = varbin.WriteUvarint(&buffer, uint64(len(s.Content)))
	if err != nil {
		return nil, err
	}
	_, err = buffer.Write(s.Content)
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buffer, binary.BigEndian, s.LastUpdated.Unix())
	if err != nil {
		return nil, err
	}
	_, err = varbin.WriteUvarint(&buffer, uint64(len(s.LastEtag)))
	if err != nil {
		return nil, err
	}
	_, err = buffer.WriteString(s.LastEtag)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (s *SavedBinary) UnmarshalBinary(data []byte) error {
	reader := bytes.NewReader(data)
	var version uint8
	err := binary.Read(reader, binary.BigEndian, &version)
	if err != nil {
		return err
	}
	contentLength, err := binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	if contentLength > uint64(reader.Len()) {
		return E.New("invalid content length: ", contentLength)
	}
	s.Content = make([]byte, contentLength)
	_, err = io.ReadFull(reader, s.Content)
	if err != nil {
		return err
	}
	var lastUpdated int64
	err = binary.Read(reader, binary.BigEndian, &lastUpdated)
	if err != nil {
		return err
	}
	s.LastUpdated = time.Unix(lastUpdated, 0)
	etagLength, err := binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	if etagLength > uint64(reader.Len()) {
		return E.New("invalid etag length: ", etagLength)
	}
	etagBytes := make([]byte, etagLength)
	_, err = io.ReadFull(reader, etagBytes)
	if err != nil {
		return err
	}
	s.LastEtag = string(etagBytes)
	return nil
}

type SmartRoutingStats struct {
	// Must be ordered from oldest to newest, such that an LRU structure
	// is populated correctly upon sequentially reading this data.
	StatsByDomains []SmartDomainStats
}

type SmartDomainStats struct {
	Domain    string
	Outbounds map[string]SmartOutboundStats
}

type SmartOutboundStats struct {
	// Must be ordered from oldest to newest, such that a ring-buffer
	// is populated correctly upon sequentially reading this data.
	RequestResults []SmartRequestResult

	LastRecheckTime time.Time // TODO: marshal and unmarshal.
}

type SmartRequestResult struct {
	Success bool
	Delay   time.Duration
}

func (s *SmartRoutingStats) MarshalBinary() ([]byte, error) {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, uint8(1))
	if err != nil {
		return nil, err
	}
	_, err = varbin.WriteUvarint(&buffer, uint64(len(s.StatsByDomains)))
	if err != nil {
		return nil, err
	}
	for _, domainState := range s.StatsByDomains {
		domain := domainState.Domain
		_, err = varbin.WriteUvarint(&buffer, uint64(len(domain)))
		if err != nil {
			return nil, err
		}
		_, err = buffer.WriteString(domain)
		if err != nil {
			return nil, err
		}
		_, err = varbin.WriteUvarint(&buffer, uint64(len(domainState.Outbounds)))
		if err != nil {
			return nil, err
		}
		for tag, stats := range domainState.Outbounds {
			_, err = varbin.WriteUvarint(&buffer, uint64(len(tag)))
			if err != nil {
				return nil, E.Cause(err, "WriteUvarint length of tag")
			}
			_, err = buffer.WriteString(tag)
			if err != nil {
				return nil, E.Cause(err, "WriteString tag")
			}
			_, err = varbin.WriteUvarint(&buffer, uint64(len(stats.RequestResults)))
			if err != nil {
				return nil, E.Cause(err, "WriteUvarint length of last requests results ring")
			}
			for _, rr := range stats.RequestResults {
				var successBit uint8
				if rr.Success {
					successBit = 1
				}
				err = buffer.WriteByte(successBit)
				if err != nil {
					return nil, err
				}
				err = binary.Write(&buffer, binary.BigEndian, rr.Delay)
				if err != nil {
					return nil, E.Cause(err, "write delay")
				}
			}
		}
	}
	return buffer.Bytes(), nil
}

func (s *SmartRoutingStats) UnmarshalBinary(data []byte) error {
	reader := bytes.NewReader(data)
	var version uint8
	err := binary.Read(reader, binary.BigEndian, &version)
	if err != nil {
		return err
	}
	domainCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return E.Cause(err, "domain count")
	}
	if domainCount > uint64(reader.Len()) {
		return E.New("invalid domain count: ", domainCount)
	}
	s.StatsByDomains = make([]SmartDomainStats, domainCount)
	for range domainCount {
		domain, err := readVarbinString(reader)
		if err != nil {
			return E.Cause(err, "read domain name")
		}
		outboundCount, err := binary.ReadUvarint(reader)
		if err != nil {
			return E.Cause(err, "read outbound count")
		}
		if outboundCount > uint64(reader.Len()) {
			return E.New("invalid outbound count: ", outboundCount)
		}
		domainState := SmartDomainStats{
			Domain:    domain,
			Outbounds: make(map[string]SmartOutboundStats, outboundCount),
		}
		for range outboundCount {
			tag, err := readVarbinString(reader)
			if err != nil {
				return E.Cause(err, "outbound tag")
			}
			resultCount, err := binary.ReadUvarint(reader)
			if err != nil {
				return E.Cause(err, "request result count")
			}
			if resultCount > uint64(reader.Len()) {
				return E.New("invalid request result count: ", resultCount)
			}
			results := make([]SmartRequestResult, resultCount)
			for i := range results {
				successBit, err := reader.ReadByte()
				if err != nil {
					return E.Cause(err, "request result success bit")
				}
				results[i].Success = successBit != 0
				err = binary.Read(reader, binary.BigEndian, &results[i].Delay)
				if err != nil {
					return E.Cause(err, "request result delay")
				}
			}
			domainState.Outbounds[tag] = SmartOutboundStats{
				RequestResults:  results,
				LastRecheckTime: time.Time{}, // FIXME(mmotyshen): actually read.
			}
		}
		s.StatsByDomains = append(s.StatsByDomains, domainState)
	}
	return nil
}

func readVarbinString(reader *bytes.Reader) (string, error) {
	stringLength, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", err
	}
	if stringLength > uint64(reader.Len()) {
		return "", E.New("string length bigger than all the data: ", stringLength)
	}
	stringData := make([]byte, stringLength)
	_, err = io.ReadFull(reader, stringData)
	if err != nil {
		return "", E.Cause(err, "read full string data")
	}
	return string(stringData), nil
}

type OutboundGroup interface {
	Outbound
	Now() string
	All() []string
}

type URLTestGroup interface {
	OutboundGroup
	URLTest(ctx context.Context) (map[string]uint16, error)
	PerformUpdateCheck()
}

func OutboundTag(detour Outbound) string {
	if group, isGroup := detour.(OutboundGroup); isGroup {
		return group.Now()
	}
	return detour.Tag()
}
