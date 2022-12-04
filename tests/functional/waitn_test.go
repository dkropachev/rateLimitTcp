package functional_test

import (
	"context"
	"testing"
	"time"

	"github.com/dkropachev/rateLimitTcp"
	"github.com/stretchr/testify/assert"
)

func RunTest(t *testing.T, limiter *rateLimitTcp.RateLimiter, testDuration time.Duration, clientsCount int, confidenceInterval float64) {
	clients := NewMockWaitNClients(limiter, clientsCount)
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	clients.StartAll(ctx).Wait()
	defer cancel()

	effectiveLimit := float64(limiter.GetGlobalLimit())
	connectionLimit := float64(limiter.GetPerConnectionLimit()) * float64(clientsCount)
	if connectionLimit < effectiveLimit {
		effectiveLimit = connectionLimit
	}
	targetBytesToBeWritten := float64(testDuration/time.Second) * effectiveLimit

	assert.InDelta(t, targetBytesToBeWritten, clients.GetTotalBytesWritten().Int64(), targetBytesToBeWritten*confidenceInterval/100)
}

func TestWaitNFitness(t *testing.T) {
	limiter := rateLimitTcp.NewRateLimiter(1000, 100, 1000, 100)
	RunTest(t, limiter, time.Second*30, 100, 5)
}
