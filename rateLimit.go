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

func (l *RateLimiter) SetConnectionGCLimit(newLimit int) {
	l.perConnectionLimitersCollectedLimit = newLimit
}

func (l *RateLimiter) GetConnectionGCLimit() int {
	return l.perConnectionLimitersCollectedLimit
}

func (l *RateLimiter) SetConnectionKeepAlive(val time.Duration) {
	l.perConnectionLimitersKeepAlive = val
}

func (l *RateLimiter) GetConnectionKeepAlive() time.Duration {
	return l.perConnectionLimitersKeepAlive
}

func (l *RateLimiter) SetGlobalLimit(newLimit rate.Limit) {
	l.globalLimiter.SetLimit(newLimit)
}

func (l *RateLimiter) GetGlobalLimit() rate.Limit {
	return l.globalLimiter.Limit()
}

func (l *RateLimiter) SetGlobalBurst(newBurst int) {
	l.globalLimiter.SetBurst(newBurst)
}

func (l *RateLimiter) GetGlobalBurst() int {
	return l.globalLimiter.Burst()
}

func (l *RateLimiter) GetPerConnectionLimitersCount() int {
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	return len(l.perConnectionLimiters)
}

func (l *RateLimiter) IsGCRunning() bool {
	res := l.perConnectionLimitersGCLock.TryLock()
	if res {
		l.perConnectionLimitersGCLock.Unlock()
		return false
	}
	return true
}

func (l *RateLimiter) TickPerConnectionLimiterClosedCounter() {
	res := atomic.AddInt32(&l.perConnectionLimitersClosed, 1)
	if l.perConnectionLimitersCollectedLimit != 0 && res == int32(l.perConnectionLimitersCollectedLimit) {
		atomic.StoreInt32(&l.perConnectionLimitersClosed, 0)
		go l.RunGC(l.perConnectionLimitersKeepAlive)
	}
}

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

func (l *RateLimiter) GetPerConnectionLimitAndBurst() (rate.Limit, int) {
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	return l.perConnectionLimit, l.perConnectionBurst
}

func (l *RateLimiter) RunGC(shift time.Duration) {
	if !l.perConnectionLimitersGCLock.TryLock() {
		return
	}
	defer l.perConnectionLimitersGCLock.Unlock()
	l.perConnectionLimitersLock.Lock()
	defer l.perConnectionLimitersLock.Unlock()
	for key, lock := range l.perConnectionLimiters {
		if lock.closedFor(shift) {
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

func (l *RateLimiter) GetPerConnectionBurst() int {
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	return l.perConnectionBurst
}

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

func (l *RateLimiter) GetPerConnectionLimit() rate.Limit {
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	return l.perConnectionLimit
}

func (l *RateLimiter) IsGlobalLimitReached() bool {
	// Check if global limit can handle 1 second of new connection
	// In case you want to limit it
	return !l.globalLimiter.AllowN(time.Now().UTC(), int(l.perConnectionLimit))
}

func (l *RateLimiter) IsGlobalLimitAllowN(bytesNum int) bool {
	// Check if global limit can handle 1 second of new connection
	// In case you want to limit it
	return !l.globalLimiter.AllowN(time.Now().UTC(), bytesNum)
}

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
