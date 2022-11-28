package functional_test

import (
	"context"
	"math/big"
	"math/rand"
	"sync"

	"github.com/dkropachev/rateLimitTcp"
	testutils "github.com/dkropachev/rateLimitTcp/tests/utils"
)

type MockClient struct {
	idx          string
	connLimiter  *rateLimitTcp.PerConnectionLimiter
	writeLimit   int
	bytesWritten int64
}

func NewClient(limiter *rateLimitTcp.RateLimiter) *MockClient {
	clientIdx := testutils.RandString(15)
	return &MockClient{
		idx:         clientIdx,
		connLimiter: limiter.GetPerConnectionLimiter(clientIdx),
		writeLimit:  int(limiter.GetPerConnectionLimit()) - 1,
	}
}

func (cl *MockClient) Run(ctx context.Context, wg *sync.WaitGroup) {
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

func (cl *MockClient) GetBytesWritten() int64 {
	return cl.bytesWritten
}

type MockClients []*MockClient

func (l MockClients) StartAll(ctx context.Context) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	wg.Add(len(l))
	for _, cl := range l {
		go cl.Run(ctx, &wg)
	}
	return &wg
}

func (l MockClients) GetTotalBytesWritten() *big.Int {
	out := big.NewInt(0)
	for _, cl := range l {
		out.Add(out, big.NewInt(cl.GetBytesWritten()))
	}
	return out
}
