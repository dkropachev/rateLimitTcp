package functional_test

import (
	"testing"
	"time"

	"github.com/dkropachev/rateLimitTcp"
	testutils "github.com/dkropachev/rateLimitTcp/tests/utils"
	"github.com/stretchr/testify/assert"
)

func TestGCManual(t *testing.T) {
	t.Parallel()
	limiter := rateLimitTcp.NewRateLimiter(1000, 1000, 1000, 1000)
	for n := 0; n < 1000; n++ {
		limiter.GetPerConnectionLimiter(testutils.RandString(100)).Close()
	}
	assert.InDelta(t, 1000, limiter.GetPerConnectionLimitersCount(), 10)
	time.Sleep(time.Millisecond * 10)
	limiter.RunGC(time.Millisecond * 10)
	limiter.GetPerConnectionLimitersCount()
	assert.Equal(t, 0, limiter.GetPerConnectionLimitersCount())
}

func TestGCCalledOnClose(t *testing.T) {
	t.Parallel()
	limiter := rateLimitTcp.NewRateLimiter(1000, 1000, 1000, 1000)
	limiter.SetConnectionKeepAlive(time.Millisecond * 10)
	limiter.SetConnectionGCLimit(1000)
	for n := 0; n < 1000; n++ {
		limiter.GetPerConnectionLimiter(testutils.RandString(100)).Close()
	}
	time.Sleep(time.Millisecond * 10)
	assert.InDelta(t, 1000, limiter.GetPerConnectionLimitersCount(), 1)
	for n := 0; n < 1000; n++ {
		limiter.GetPerConnectionLimiter(testutils.RandString(100)).Close()
	}
	time.Sleep(time.Millisecond * 10)
	for limiter.IsGCRunning() {
		time.Sleep(time.Millisecond * 10)
	}
	limiter.GetPerConnectionLimitersCount()
	assert.Equal(t, 1000, limiter.GetPerConnectionLimitersCount())
}
