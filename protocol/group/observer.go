package group

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/sagernet/tailscale/types/ptr"
)

type connObserver struct {
	net.Conn
	group  *SmartGroup
	domain string
	tag    string

	requestAt   time.Time // Initialize with the observer.
	firstReadAt atomic.Pointer[time.Time]
	reported    atomic.Bool
}

func newConnObserver(group *SmartGroup, domain, tag string, conn net.Conn) net.Conn {
	return &connObserver{
		Conn:      conn,
		group:     group,
		domain:    domain,
		tag:       tag,
		requestAt: time.Now(),
	}
}

func (c *connObserver) Upstream() any {
	return c.Conn
}

func (c *connObserver) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if n > 0 {
		c.firstReadAt.CompareAndSwap(nil, ptr.To(time.Now()))
	}
	return
}

func (c *connObserver) Close() error {
	c.report()
	return c.Conn.Close()
}

func (c *connObserver) report() {
	swapped := c.reported.CompareAndSwap(false, true)
	if !swapped {
		return
	}
	firstReadAt := c.firstReadAt.Load()
	if firstReadAt == nil {
		c.group.observe(c.domain, c.tag, false, 0)
		return
	}
	timeToFirstByte := firstReadAt.Sub(c.requestAt)
	c.group.observe(c.domain, c.tag, true, timeToFirstByte)
	// TODO(mmotyshen): add debug-logs.
}
