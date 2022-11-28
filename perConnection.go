package rateLimitTcp

import (
	"context"
	"net"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/time/rate"
)

type PerConnectionLimiter struct {
	key          string
	localLimiter *rate.Limiter
	parent       *RateLimiter
	closeTime    *time.Time
}

// Open marks limiter as currently in use, to avoid it being collected by GC
func (pcl *PerConnectionLimiter) Open() *PerConnectionLimiter {
	pcl.closeTime = nil
	return pcl
}

// Close marks limiter as not in use, to enable it being collected by GC
func (pcl *PerConnectionLimiter) Close() *PerConnectionLimiter {
	now := time.Now().UTC()
	val := (*time.Time)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&pcl.closeTime))))
	if val == nil {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&pcl.closeTime)), unsafe.Pointer(&now))
		pcl.parent.tickPerConnectionLimiterClosedCounter()
	}
	return pcl
}

func (pcl *PerConnectionLimiter) closedFor(shift time.Duration) bool {
	closeTime := (*time.Time)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&pcl.closeTime))))
	if closeTime == nil {
		return false
	}
	return closeTime.Before(time.Now().UTC().Add(-shift))
}

// AllowN analogue of `AllowN` of `rate` package, but takes into account both global and connection limiter
func (pcl *PerConnectionLimiter) AllowN(t time.Time, n int) bool {
	return pcl.parent.globalLimiter.AllowN(t, n) && pcl.localLimiter.AllowN(t, n)
}

// Allow analogue of `Allow` of `rate` package, but takes into account both global and connection limiter
func (pcl *PerConnectionLimiter) Allow() bool {
	return pcl.AllowN(time.Now().UTC(), 1)
}

// WaitN analogue of `WaitN` of `rate` package, but takes into account both global and connection limiter
func (pcl *PerConnectionLimiter) WaitN(ctx context.Context, n int) error {
	err := pcl.parent.globalLimiter.WaitN(ctx, n)
	if err != nil {
		return err
	}
	return pcl.localLimiter.WaitN(ctx, n)
}

// Wait analogue of `Wait` of `rate` package, but takes into account both global and connection limiter
func (pcl *PerConnectionLimiter) Wait(ctx context.Context) error {
	return pcl.WaitN(ctx, 1)
}

// Burst analogue of `Burst` of `rate` package, but takes into account both global and connection limiter, returns the lowest value
func (pcl *PerConnectionLimiter) Burst() int {
	gb := pcl.parent.globalLimiter.Burst()
	lb := pcl.localLimiter.Burst()
	if gb > lb {
		return lb
	}
	return gb
}

// TokensAt analogue of `TokensAt` of `rate` package, but takes into account both global and connection limiter, returns the lowest value
func (pcl *PerConnectionLimiter) TokensAt(t time.Time) float64 {
	gt := pcl.parent.globalLimiter.TokensAt(t)
	lt := pcl.localLimiter.TokensAt(t)
	if gt > lt {
		return lt
	}
	return gt
}

// Tokens analogue of `Tokens` of `rate` package, but takes into account both global and connection limiter, returns the lowest value
func (pcl *PerConnectionLimiter) Tokens() float64 {
	return pcl.TokensAt(time.Now().UTC())
}

// Limit analogue of `Limit` of `rate` package, but takes into account both global and connection limiter, returns the lowest value
func (pcl *PerConnectionLimiter) Limit() rate.Limit {
	gl := pcl.parent.globalLimiter.Limit()
	ll := pcl.localLimiter.Limit()
	if gl > ll {
		return ll
	}
	return ll
}

func (pcl *PerConnectionLimiter) setLimit(newLimit rate.Limit) {
	pcl.localLimiter.SetLimit(newLimit)
}

func (pcl *PerConnectionLimiter) setBurst(newBurst int) {
	pcl.localLimiter.SetBurst(newBurst)
}

// WrappedNetConnection wraps net.Conn to implement seamless rate limiting over net.Conn interface
// Don't forget to tweak it with `SetDefaultTimeout` and `SetDirections` if necessary
func (pcl *PerConnectionLimiter) WrappedNetConnection(conn net.Conn) *WrappedNetConnection {
	return &WrappedNetConnection{
		real:    conn,
		limiter: pcl,
	}
}

type WrappedNetConnection struct {
	real           net.Conn
	limiter        *PerConnectionLimiter
	defaultTimeout time.Duration
	directions     Direction
}

// SetDefaultTimeout set default timeout that is applied to reads/writes while waiting for limit
// It is not thread safe, use it once, when connection just wrapped
func (wc *WrappedNetConnection) SetDefaultTimeout(timeout time.Duration) *WrappedNetConnection {
	wc.defaultTimeout = timeout
	return wc
}

// SetDirections set direction to limit,
// `Inbound` - Reads are limited, data could be lost when timeout is reached
// `Outbound` - Writes are limited, operation returns error when timeout is reached
// You can use bitwise or `|` to limit both directions
func (wc *WrappedNetConnection) SetDirections(direction Direction) *WrappedNetConnection {
	wc.directions = direction
	return wc
}

func (wc *WrappedNetConnection) Read(b []byte) (int, error) {
	var cancel context.CancelFunc

	out := make([]byte, len(b))
	n, err := wc.real.Read(out)
	if !wc.directions.IsInbound() || err != nil {
		return n, err
	}
	ctx := context.Background()
	if wc.defaultTimeout != time.Duration(0) {
		ctx, cancel = context.WithTimeout(ctx, wc.defaultTimeout)
		defer cancel()
	}
	err = wc.limiter.WaitN(ctx, len(b))
	if err != nil {
		return 0, err
	}
	copy(b, out)
	return n, nil
}

func (wc *WrappedNetConnection) Write(b []byte) (int, error) {
	var cancel context.CancelFunc

	if !wc.directions.IsOutbound() {
		return wc.real.Write(b)
	}
	ctx := context.Background()
	if wc.defaultTimeout != time.Duration(0) {
		ctx, cancel = context.WithTimeout(ctx, wc.defaultTimeout)
		defer cancel()
	}
	err := wc.limiter.WaitN(ctx, len(b))
	if err != nil {
		return 0, err
	}
	return wc.real.Write(b)
}

// Close closes wrapped connection and marks limiter as closed
// can potentially trigger per connection limiters GC, which is run on the background
func (wc *WrappedNetConnection) Close() error {
	wc.limiter.Close()
	return wc.real.Close()
}

func (wc *WrappedNetConnection) LocalAddr() net.Addr {
	return wc.real.LocalAddr()
}

func (wc *WrappedNetConnection) RemoteAddr() net.Addr {
	return wc.real.RemoteAddr()
}

func (wc *WrappedNetConnection) SetDeadline(t time.Time) error {
	return wc.real.SetDeadline(t)
}

func (wc *WrappedNetConnection) SetReadDeadline(t time.Time) error {
	return wc.real.SetReadDeadline(t)
}

func (wc *WrappedNetConnection) SetWriteDeadline(t time.Time) error {
	return wc.real.SetWriteDeadline(t)
}
