package rateLimitTcp

import (
	"context"
	"net"
	"sync"
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
	wg := sync.WaitGroup{}
	wg.Add(2)
	var gErr error
	var lErr error
	go func() {
		gErr = pcl.parent.globalLimiter.WaitN(ctx, n)
		wg.Done()
	}()
	go func() {
		lErr = pcl.localLimiter.WaitN(ctx, n)
		wg.Done()
	}()
	wg.Wait()

	if gErr != nil {
		return gErr
	}
	if lErr != nil {
		return lErr
	}
	return nil
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

// ReserveN analogue of `ReserveN` of `rate` package, but gets both global and connection reservations and compounds them
func (pcl *PerConnectionLimiter) ReserveN(t time.Time, n int) *WrappedReservation {
	gr := pcl.parent.globalLimiter.ReserveN(t, n)
	if !gr.OK() {
		return &WrappedReservation{global: gr}
	}
	return &WrappedReservation{
		global: gr,
		local:  pcl.localLimiter.ReserveN(t, n),
	}
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
