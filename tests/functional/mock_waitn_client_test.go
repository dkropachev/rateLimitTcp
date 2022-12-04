package functional_test

import (
	"context"
	"math/big"
	"math/rand"
	"sync"

	"github.com/dkropachev/rateLimitTcp"
	testutils "github.com/dkropachev/rateLimitTcp/tests/utils"
)

type MockWaitNClient struct {
	idx          string
	connLimiter  *rateLimitTcp.PerConnectionLimiter
	writeLimit   int
	bytesWritten int64
}

func NewMockWaitNClient(limiter *rateLimitTcp.RateLimiter) *MockWaitNClient {
	clientIdx := testutils.RandString(15)
	return &MockWaitNClient{
		idx:         clientIdx,
		connLimiter: limiter.GetPerConnectionLimiter(clientIdx),
		writeLimit:  int(limiter.GetPerConnectionLimit()) - 1,
	}
}

func (cl *MockWaitNClient) Run(ctx context.Context, wg *sync.WaitGroup) {
	cl.connLimiter.Open()
	defer cl.connLimiter.Close()
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

func (cl *MockWaitNClient) GetBytesWritten() int64 {
	return cl.bytesWritten
}

type MockWaitNClients []*MockWaitNClient

func NewMockWaitNClients(limiter *rateLimitTcp.RateLimiter, n int) MockWaitNClients {
	clients := make([]*MockWaitNClient, n)
	for k := range clients {
		clients[k] = NewMockWaitNClient(limiter)
	}
	return clients
}

func (l MockWaitNClients) StartAll(ctx context.Context) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	wg.Add(len(l))
	for _, cl := range l {
		go cl.Run(ctx, &wg)
	}
	return &wg
}

func (l MockWaitNClients) GetTotalBytesWritten() *big.Int {
	out := big.NewInt(0)
	for _, cl := range l {
		out.Add(out, big.NewInt(cl.GetBytesWritten()))
	}
	return out
}
