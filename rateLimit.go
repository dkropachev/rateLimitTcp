package rateLimitTcp

import (
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

type Direction int

const (
	Inbound  = Direction(1)
	Outbound = Direction(2)

	defaultGCConnectionLimit     = 100
	defaultGCConnectionKeepAlive = time.Minute * 2
)

func (d Direction) IsInbound() bool {
	return (d & Inbound) == 1
}

func (d Direction) IsOutbound() bool {
	return (d & Outbound) == 1
}

type RateLimiter struct {
	globalLimiter                       *rate.Limiter
	perConnectionLimiters               map[string]*PerConnectionLimiter
	perConnectionLimitersLock           sync.RWMutex
	perConnectionLimit                  rate.Limit
	perConnectionBurst                  int
	perConnectionLimitersCollected      int
	perConnectionLimitersCollectedLimit int
	perConnectionLimitersKeepAlive      time.Duration
	perConnectionLimitersClosed         int32
	perConnectionLimitersGCLock         sync.Mutex
}

func NewRateLimiter(globalLimit, perConnectionLimit rate.Limit, globalBurst int, perConnectionBurst int) *RateLimiter {
	return &RateLimiter{
		globalLimiter:                       rate.NewLimiter(globalLimit, globalBurst),
		perConnectionLimit:                  perConnectionLimit,
		perConnectionBurst:                  perConnectionBurst,
		perConnectionLimiters:               make(map[string]*PerConnectionLimiter),
		perConnectionLimitersCollectedLimit: defaultGCConnectionLimit,
		perConnectionLimitersKeepAlive:      defaultGCConnectionKeepAlive,
	}
}

//SetConnectionGCLimit set number of connections to be closed to trigger GC
// same limit is applied to number of deallocated limiters by GC to trigger `perConnectionLimiters` recreation
// It is not thread safe
func (l *RateLimiter) SetConnectionGCLimit(newLimit int) {
	l.perConnectionLimitersCollectedLimit = newLimit
}

//GetConnectionGCLimit get number of connections to be closed to trigger GC
// It is not thread safe
func (l *RateLimiter) GetConnectionGCLimit() int {
	return l.perConnectionLimitersCollectedLimit
}

// SetConnectionKeepAlive set time for how long closed connection limiter is going to be kept, it is not thread safe
func (l *RateLimiter) SetConnectionKeepAlive(val time.Duration) {
	l.perConnectionLimitersKeepAlive = val
}

// GetConnectionKeepAlive get time for how long closed connection limiter is going to be kept, it is not thread safe
func (l *RateLimiter) GetConnectionKeepAlive() time.Duration {
	return l.perConnectionLimitersKeepAlive
}

// SetGlobalLimit get global limiter limit, it is thread safe
func (l *RateLimiter) SetGlobalLimit(newLimit rate.Limit) {
	l.globalLimiter.SetLimit(newLimit)
}

// GetGlobalLimit get global limiter limit, it is thread safe
func (l *RateLimiter) GetGlobalLimit() rate.Limit {
	return l.globalLimiter.Limit()
}

// SetGlobalBurst set global limiter burst, it is thread safe
func (l *RateLimiter) SetGlobalBurst(newBurst int) {
	l.globalLimiter.SetBurst(newBurst)
}

// GetGlobalBurst get global limiter burst, it is thread safe
func (l *RateLimiter) GetGlobalBurst() int {
	return l.globalLimiter.Burst()
}

// GetPerConnectionLimitersCount return number of per connection limiters, it is thread safe
func (l *RateLimiter) GetPerConnectionLimitersCount() int {
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	return len(l.perConnectionLimiters)
}

// IsGCRunning return try if GC is currently running, it is thread safe
func (l *RateLimiter) IsGCRunning() bool {
	res := l.perConnectionLimitersGCLock.TryLock()
	if res {
		l.perConnectionLimitersGCLock.Unlock()
		return false
	}
	return true
}

// tickPerConnectionLimiterClosedCounter updates counter of closed connections
// and trigger GC when `perConnectionLimitersCollectedLimit` is reached
// it is thread safe
func (l *RateLimiter) tickPerConnectionLimiterClosedCounter() {
	res := atomic.AddInt32(&l.perConnectionLimitersClosed, 1)
	if l.perConnectionLimitersCollectedLimit != 0 && res == int32(l.perConnectionLimitersCollectedLimit) {
		atomic.StoreInt32(&l.perConnectionLimitersClosed, 0)
		go l.RunGC(l.perConnectionLimitersKeepAlive)
	}
}

// SetPerConnectionLimitAndBurst set per connection limit and burst, update currently registered limiters, it is thread safe
func (l *RateLimiter) SetPerConnectionLimitAndBurst(newLimit rate.Limit, newBurst int) {
	l.perConnectionLimitersLock.Lock()
	l.perConnectionLimit = newLimit
	l.perConnectionLimitersLock.Unlock()
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	for _, lock := range l.perConnectionLimiters {
		if lock != nil {
			lock.setLimit(newLimit)
			lock.setBurst(newBurst)
		}
	}
}

// GetPerConnectionLimitAndBurst get per connection limit and burst, it is thread safe
func (l *RateLimiter) GetPerConnectionLimitAndBurst() (rate.Limit, int) {
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	return l.perConnectionLimit, l.perConnectionBurst
}

// RunGC goes over registered limiters, find ones that where closed some time (`period`) ago and remove them from the
// `perConnectionLimiters`, once in a while it recreates `perConnectionLimiters` in order to shrink it
func (l *RateLimiter) RunGC(period time.Duration) {
	if !l.perConnectionLimitersGCLock.TryLock() {
		return
	}
	defer l.perConnectionLimitersGCLock.Unlock()
	l.perConnectionLimitersLock.Lock()
	defer l.perConnectionLimitersLock.Unlock()
	for key, lock := range l.perConnectionLimiters {
		if lock.closedFor(period) {
			l.perConnectionLimiters[key] = nil
			l.perConnectionLimitersCollected++
		}
	}
	if l.perConnectionLimitersCollected >= l.perConnectionLimitersCollectedLimit {
		// Recreate l.perConnectionLimiters since maps do not shrink
		oldMap := l.perConnectionLimiters
		l.perConnectionLimiters = make(map[string]*PerConnectionLimiter, len(oldMap)-l.perConnectionLimitersCollected)
		for key, lock := range oldMap {
			if lock != nil {
				l.perConnectionLimiters[key] = lock
			}
		}
		l.perConnectionLimitersCollected = 0
	}
}

// SetPerConnectionBurst set per connection burst and update currently registered limiters, it is thread safe
func (l *RateLimiter) SetPerConnectionBurst(newBurst int) {
	l.perConnectionLimitersLock.Lock()
	l.perConnectionBurst = newBurst
	l.perConnectionLimitersLock.Unlock()
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	for _, lock := range l.perConnectionLimiters {
		if lock != nil {
			lock.setBurst(newBurst)
		}
	}
}

// GetPerConnectionBurst get per connection burst, it is thread safe
func (l *RateLimiter) GetPerConnectionBurst() int {
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	return l.perConnectionBurst
}

// SetPerConnectionLimit set per connection limit, and update currently registered limiters, it is thread safe
func (l *RateLimiter) SetPerConnectionLimit(newLimit rate.Limit) {
	l.perConnectionLimitersLock.Lock()
	l.perConnectionLimit = newLimit
	l.perConnectionLimitersLock.Unlock()
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	for _, lock := range l.perConnectionLimiters {
		if lock != nil {
			lock.setLimit(newLimit)
		}
	}
}

// GetPerConnectionLimit get per connection limit, it is thread safe
func (l *RateLimiter) GetPerConnectionLimit() rate.Limit {
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	return l.perConnectionLimit
}

// GetGlobalLimiter get global limiter, this limiter never changes, so it is thread safe
func (l *RateLimiter) GetGlobalLimiter() *rate.Limiter {
	return l.globalLimiter
}

func (l *RateLimiter) newPerConnectionLimit(key string) *PerConnectionLimiter {
	return &PerConnectionLimiter{
		key:          key,
		localLimiter: rate.NewLimiter(l.perConnectionLimit, l.perConnectionBurst),
		parent:       l,
	}
}

// GetPerConnectionLimiter get per connection limiter, create it if it does not exist, it is thread safe
func (l *RateLimiter) GetPerConnectionLimiter(key string) *PerConnectionLimiter {
	l.perConnectionLimitersLock.RLock()
	limiter := l.perConnectionLimiters[key]
	l.perConnectionLimitersLock.RUnlock()
	if limiter != nil {
		return limiter
	}

	l.perConnectionLimitersLock.Lock()
	defer l.perConnectionLimitersLock.Unlock()

	limiter = l.perConnectionLimiters[key]
	if limiter != nil {
		return limiter
	}
	limiter = l.newPerConnectionLimit(key)
	l.perConnectionLimiters[key] = limiter
	return limiter
}
