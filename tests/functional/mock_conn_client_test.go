package functional_test

import (
	"context"
	"math/big"
	"sync"

	"github.com/dkropachev/rateLimitTcp"
	testutils "github.com/dkropachev/rateLimitTcp/tests/utils"
)

type MockConnClient struct {
	idx          string
	connLimiter  *rateLimitTcp.PerConnectionLimiter
	conn         *rateLimitTcp.WrappedNetConnection
	writeLimit   int
	bytesWritten int64
	bytesRead    int64
	direction    rateLimitTcp.Direction
}

func NewMockConnClient(limiter *rateLimitTcp.RateLimiter, direction rateLimitTcp.Direction) *MockConnClient {
	clientIdx := testutils.RandString(15)
	return &MockConnClient{
		idx:         clientIdx,
		connLimiter: limiter.GetPerConnectionLimiter(clientIdx),
		writeLimit:  int(limiter.GetPerConnectionLimit()) - 1,
		direction:   direction,
	}
}

func (cl *MockConnClient) Run(ctx context.Context, wg *sync.WaitGroup) {
	cl.connLimiter.Open()
	defer cl.connLimiter.Close()
	defer wg.Done()
	localWg := sync.WaitGroup{}
	if cl.direction.IsInbound() {
		localWg.Add(1)
		go cl.readCycle(ctx, &localWg)
	}
	if cl.direction.IsOutbound() {
		localWg.Add(1)
		go cl.writeCycle(ctx, &localWg)
	}
}

func (cl *MockConnClient) readCycle(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	buff := make([]byte, 1024)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, _ := cl.conn.Read(buff)
		cl.bytesRead += int64(n)
	}
}

func (cl *MockConnClient) writeCycle(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	buff := []byte(testutils.RandString(1024))
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, _ := cl.conn.Write(buff)
		cl.bytesWritten += int64(n)
	}
}

func (cl *MockConnClient) GetBytesWritten() int64 {
	return cl.bytesWritten
}

func (cl *MockConnClient) GetBytesRead() int64 {
	return cl.bytesRead
}

type MockConnClients []*MockConnClient

func (l MockConnClients) StartAll(ctx context.Context) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	wg.Add(len(l))
	for _, cl := range l {
		go cl.Run(ctx, &wg)
	}
	return &wg
}

func (l MockConnClients) GetTotalBytesWritten() *big.Int {
	out := big.NewInt(0)
	for _, cl := range l {
		out.Add(out, big.NewInt(cl.GetBytesWritten()))
	}
	return out
}

func (l MockConnClients) GetTotalBytesRead() *big.Int {
	out := big.NewInt(0)
	for _, cl := range l {
		out.Add(out, big.NewInt(cl.GetBytesWritten()))
	}
	return out
}
