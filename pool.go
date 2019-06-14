package netng

import (
	"context"
	"math"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beorn7/perks/quantile"
)

type bufQuantile interface {
	Observe(float64)
	Results() map[float64]float64
	Count() int
}

type internalPool interface {
	Get(context.Context) (net.Conn, error)
	Put(context.Context, *conn)
}

type pool struct {
	conns           chan *conn
	connsRef        []*conn
	connsMut        sync.Mutex
	maxSize         int
	dial            func() (net.Conn, error)
	opts            *PoolOptions
	bufSizeQuantile bufQuantile
	quantileMut     sync.Mutex
	quantileValue   atomic.Value
	targetBufSize   int
}

// PoolOptions is specified to tune the connection pool for the client
type PoolOptions struct {
	MaxSize         int                      // The maximum size of the connection pool. If reached, it will block the next client request until a connection is free
	MaxIdle         int                      // How many connections that can remain idle in the pool, will otherwise be reaped by the trimming thread
	ReadTimeout     time.Duration            // Default is 500ms
	Dial            func() (net.Conn, error) // a function that returns an established TCP connection
	InitialBufSize  int                      // The initial buffer size to associate with the connection, this is also the minimum buffer size allowed when creating new connections, but if the trimming thread is enabled and the percentile target returns a higher value, this will be used for any subsequent connections
	TrimOptions     *TrimOptions             // fine-tuning options for the trimming thread, this should usually not be needed
	bufSizeQuantile bufQuantile
}

// NewPool creates a new pool
func NewPool(ctx context.Context, opts *PoolOptions) internalPool {
	opts.TrimOptions = initTrimOptions(opts.TrimOptions)
	if opts.InitialBufSize < 1 {
		opts.InitialBufSize = 4096
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = 500 * time.Millisecond
	}
	var p = &pool{
		conns:           make(chan *conn, opts.MaxSize),
		maxSize:         opts.MaxSize,
		dial:            opts.Dial,
		connsMut:        sync.Mutex{},
		bufSizeQuantile: newQuantileStream(opts.TrimOptions.BufQuantileTargets),
		targetBufSize:   opts.InitialBufSize,
		opts:            opts,
	}
	if opts.TrimOptions.Interval > 0 {
		go connTrimming(ctx, time.Tick(opts.TrimOptions.Interval), p)

	}
	return p
}

// calcTargetBufSize takes a quantile and selects a sane new target []byte buffer size for connections in the pool
// it takes several measures to ensure that it doesn't thrash too much on the targetBufSize
func (p *pool) calcTargetBufSize(q bufQuantile, target float64) int {
	// only spend time if we have enough samples
	if q.Count() >= 100 {
		var results = q.Results()
		var targetResult = results[target]
		if targetResult < float64(p.opts.InitialBufSize) {
			// make sure we don't reduce buf size below initial size
			return p.opts.InitialBufSize
		}
		if math.Abs(targetResult-float64(p.targetBufSize)) < targetResult*0.1 {
			// avoid too much bouncing of targetBufSize
			return p.targetBufSize
		}
		var orderedResults = make([]float64, 0, len(results))
		for q := range results {
			orderedResults = append(orderedResults, q)
		}
		sort.Sort(sort.Reverse(sort.Float64Slice(orderedResults)))
		var lastVal float64
		for _, percentile := range orderedResults {
			lastVal = results[percentile]
			if targetResult+targetResult*p.opts.TrimOptions.AllowedMargin > lastVal {
				return int(lastVal)
			}
		}
	}
	return p.opts.InitialBufSize
}

func (p *pool) Get(ctx context.Context) (net.Conn, error) {
	var c *conn
	var err error
	select {
	case c = <-p.conns:
		if c.getToBeClosed() {
			var nc net.Conn
			nc, err = p.Get(ctx)
			c = nc.(*conn)
		}
		c.setInUse(true)
	default:
		var conn net.Conn
		conn, err = p.dial()
		if err != nil {
			return nil, err
		}
		c = newConn(conn, p.targetBufSize)
		p.connsMut.Lock()
		p.connsRef = append(p.connsRef, c)
		p.connsMut.Unlock()
	}
	return c, err
}

func (p *pool) Put(ctx context.Context, c *conn) {
	c.setInUse(false)
	if c.getToBeClosed() {
		return
	}
	p.bufSizeQuantile.Observe(float64(cap(c.buf)))
	select {
	case p.conns <- c:
	default:
	}
}

type quantileStream struct {
	targets []float64
	q       *quantile.Stream
	mut     sync.RWMutex
}

func sliceToTargets(slice []float64) map[float64]float64 {
	var formattedTarget = make(map[float64]float64, len(slice))
	for _, target := range slice {
		formattedTarget[target] = 1 - target
		if target != 1 {
			formattedTarget[target] = formattedTarget[target] / 10
		}
	}
	return formattedTarget
}

func newQuantileStream(targets []float64) bufQuantile {
	var q = &quantileStream{
		targets: targets,
		q:       quantile.NewTargeted(sliceToTargets(targets)),
		mut:     sync.RWMutex{},
	}
	return q
}

func (q *quantileStream) Observe(v float64) {
	q.mut.Lock()
	q.q.Insert(v)
	q.mut.Unlock()
}

func (q *quantileStream) Results() map[float64]float64 {
	var result = make(map[float64]float64, len(q.targets))
	q.mut.RLock()
	for _, target := range q.targets {
		result[target] = q.q.Query(target)
	}
	q.mut.RUnlock()
	return result
}

func (q *quantileStream) Count() int {
	q.mut.RLock()
	var count = q.q.Count()
	q.mut.RUnlock()
	return count
}
