package rateLimitTcp

import (
	"golang.org/x/time/rate"
	"sync"
	"time"
)

type Direction int

const (
	Inbound  = Direction(1)
	Outbound = Direction(2)
)

func (d Direction) IsInbound() bool {
	return (d & Inbound) == 1
}

func (d Direction) IsOutbound() bool {
	return (d & Outbound) == 1
}

type RateLimiter struct {
	globalLimiter             *rate.Limiter
	perConnectionLimiters     map[string]*PerConnectionLimiter
	perConnectionLimitersLock sync.RWMutex
	perConnectionLimit        rate.Limit
	perConnectionBurst        int
}

func NewRateLimiter(globalLimit, perConnectionLimit rate.Limit, globalBurst int, perConnectionBurst int) *RateLimiter {
	return &RateLimiter{
		globalLimiter:         rate.NewLimiter(globalLimit, globalBurst),
		perConnectionLimit:    perConnectionLimit,
		perConnectionBurst:    perConnectionBurst,
		perConnectionLimiters: make(map[string]*PerConnectionLimiter),
	}
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

func (l *RateLimiter) SetPerConnectionLimitAndBurst(newLimit rate.Limit, newBurst int) {
	l.perConnectionLimitersLock.Lock()
	l.perConnectionLimit = newLimit
	l.perConnectionLimitersLock.Unlock()
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	for _, lock := range l.perConnectionLimiters {
		lock.setLimit(newLimit)
		lock.setBurst(newBurst)
	}
}

func (l *RateLimiter) GetPerConnectionLimitAndBurst() (rate.Limit, int) {
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	return l.perConnectionLimit, l.perConnectionBurst
}

func (l *RateLimiter) SetPerConnectionBurst(newBurst int) {
	l.perConnectionLimitersLock.Lock()
	l.perConnectionBurst = newBurst
	l.perConnectionLimitersLock.Unlock()
	l.perConnectionLimitersLock.RLock()
	defer l.perConnectionLimitersLock.RUnlock()
	for _, lock := range l.perConnectionLimiters {
		lock.setBurst(newBurst)
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
		lock.setLimit(newLimit)
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

func (l *RateLimiter) newPerConnectionLimit() *PerConnectionLimiter {
	return &PerConnectionLimiter{
		localLimiter:  rate.NewLimiter(l.perConnectionLimit, l.perConnectionBurst),
		globalLimiter: l.globalLimiter,
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
	limiter = l.newPerConnectionLimit()
	l.perConnectionLimiters[key] = limiter
	return limiter
}
