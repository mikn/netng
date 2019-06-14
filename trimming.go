package netng

// TrimOptions represents advanced options to fine-tune the trimming background thread for connections
type TrimOptions struct {
	Interval           time.Duration // default is 500 ms
	BufQuantileTargets []float64     // the targets to track in 0-1 percentiles, if none given, the default is []float{0.8, 1}
	BufQuantileTarget  float64       // The specific target for buf size. If invalid or omitted, it will pick the first (a[0]) percentile from BufQuantileTargets
	AllowedMargin      float64       // this is 0-1 representing a fraction of how far from the lowest percentile a higher one can be so that it shifts to using a higher percentile. To disable it, set it below 0. It can also go higher than 1, but we recommend targeting a higher percentile instead. The default is 0.1
}

func initTrimOptions(opts *TrimOptions) *TrimOptions {
	if opts == nil {
		opts = &TrimOptions{}
	}
	if opts.Interval == 0 {
		opts.Interval = 500 * time.Millisecond
	}
	if opts.BufQuantileTargets == nil {
		opts.BufQuantileTargets = []float64{0.8, 1}
	}
	var targetExists = false
	for _, v := range opts.BufQuantileTargets {
		if v == opts.BufQuantileTarget {
			targetExists = true
			break
		}
	}
	if !targetExists {
		opts.BufQuantileTarget = opts.BufQuantileTargets[0]
	}
	if opts.AllowedMargin == 0 {
		opts.AllowedMargin = 0.1
	}
	if opts.AllowedMargin < 0 {
		opts.AllowedMargin = 0
	}
	return opts
}

func connTrimming(ctx context.Context, tick <-chan time.Time, pool *pool) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			pool.connsMut.Lock()
			// check whether we have too many idle connections
			for i := len(pool.conns); i > pool.opts.MaxIdle; i-- {
				var c = <-pool.conns
				c.setToBeClosed()
			}
			// close connections scheduled for closing
			var filtered = pool.connsRef[:0] // (ab)use the relationship of slice/array
			for _, conn := range pool.connsRef {
				if conn.getToBeClosed() && !conn.getInUse() {
					conn.conn.Close()
				} else {
					filtered = append(filtered, conn)
				}
			}
			// ensure the closed connections that didn't get overwritten also gets fed to the GC
			pool.connsRef = filtered

			// work out the new target buf size
			pool.targetBufSize = pool.calcTargetBufSize(pool.bufSizeQuantile, pool.opts.TrimOptions.BufQuantileTarget)
			// Resize buffers according to targetBufSize
			for _, conn := range pool.connsRef {
				if len(conn.buf) != pool.targetBufSize && !conn.getInUse() {
					conn.buf = make([]byte, pool.targetBufSize)
				}
			}
			pool.connsMut.Unlock()
		}
	}
}

