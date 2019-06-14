package netng

import (
	"net"
	"sync/atomic"
	"time"
)

type conn struct {
	net.Conn
	buf         []byte
	initBufSize int
	created     time.Time
	lastChecked time.Time
	toBeClosed  int64 // only for use with atomics
	inUse       int64 // only for use with atomics
}

func newConn(c net.Conn, bufSize int) *conn {
	var buf = make([]byte, bufSize)
	return &conn{
		Conn:        c,
		buf:         buf,
		initBufSize: bufSize,
		created:     time.Now(),
		toBeClosed:  0,
		inUse:       0,
	}
}

func (c *conn) getToBeClosed() bool {
	return (atomic.LoadInt64(&c.toBeClosed) == 1)
}

func (c *conn) setToBeClosed() {
	atomic.StoreInt64(&c.toBeClosed, 1)
}

func (c *conn) getInUse() bool {
	return (atomic.LoadInt64(&c.inUse) == 1)
}

func (c *conn) setInUse(yes bool) {
	var truth int64
	if yes {
		truth = 1
	}
	atomic.StoreInt64(&c.inUse, truth)
}
