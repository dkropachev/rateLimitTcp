package functional_test

import (
	"context"
	"github.com/dkropachev/rateLimitTcp"
	testutils "github.com/dkropachev/rateLimitTcp/tests/utils"
	"github.com/stretchr/testify/assert"
	"math/big"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func Client() {

}

type FakeClient struct {
	idx          string
	connLimiter  *rateLimitTcp.PerConnectionLimiter
	writeLimit   int
	bytesWritten int64
}

func NewClient(limiter *rateLimitTcp.RateLimiter) *FakeClient {
	clientIdx := testutils.RandString(15)
	return &FakeClient{
		idx:         clientIdx,
		connLimiter: limiter.GetPerConnectionLimiter(clientIdx),
		writeLimit:  int(limiter.GetPerConnectionLimit()) - 1,
	}
}

func (cl *FakeClient) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		bytesToWrite := rand.Intn(cl.writeLimit)
		err := cl.connLimiter.WaitN(ctx, bytesToWrite)
		if err != nil {
			return
		}
		cl.bytesWritten += int64(bytesToWrite)
	}
}

func (cl *FakeClient) GetBytesWritten() int64 {
	return cl.bytesWritten
}

type FakeClients []*FakeClient

func (l FakeClients) StartAll(ctx context.Context) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	wg.Add(len(l))
	for _, cl := range l {
		go cl.Run(ctx, &wg)
	}
	return &wg
}

func (l FakeClients) GetTotalBytesWritten() *big.Int {
	out := big.NewInt(0)
	for _, cl := range l {
		out.Add(out, big.NewInt(cl.GetBytesWritten()))
	}
	return out
}

func CreateFakeClients(limiter *rateLimitTcp.RateLimiter, n int) FakeClients {
	clients := make([]*FakeClient, n)
	for k := range clients {
		clients[k] = NewClient(limiter)
	}
	return clients
}

func RunTest(t *testing.T, limiter *rateLimitTcp.RateLimiter, testDuration time.Duration, clientsCount int, confidenceInterval float64) {
	clients := CreateFakeClients(limiter, clientsCount)
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
