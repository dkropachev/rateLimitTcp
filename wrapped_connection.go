package rateLimitTcp

import (
	"context"
	"net"
	"sync/atomic"
	"time"
	"unsafe"
)

var neverTimeout = time.Time{}

type WrappedNetConnection struct {
	real           net.Conn
	limiter        *PerConnectionLimiter
	defaultTimeout time.Duration
	directions     Direction
	readDeadLine   *time.Time
	writeDeadLine  *time.Time
}

// SetDefaultTimeout set default timeout that is applied to reads/writes while waiting for limit
// It is not thread safe, use it once, when connection just wrapped
func (wc *WrappedNetConnection) SetDefaultTimeout(timeout time.Duration) *WrappedNetConnection {
	wc.defaultTimeout = timeout
	return wc
}

// SetDirections set direction to limit,
// `Inbound` - Reads are limited, data could be lost when timeout is reached
// `Outbound` - Writes are limited, operation returns error when timeout is reached
// You can use bitwise or `|` to limit both directions
func (wc *WrappedNetConnection) SetDirections(direction Direction) *WrappedNetConnection {
	wc.directions = direction
	return wc
}

func (wc *WrappedNetConnection) GetDirections() Direction {
	return wc.directions
}

func (wc *WrappedNetConnection) Read(b []byte) (int, error) {
	if !wc.directions.IsInbound() {
		return wc.real.Read(b)
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	readDeadLine := wc.getReadDeadLine()
	if readDeadLine != nil {
		ctx, cancel = context.WithDeadline(ctx, *readDeadLine)
		defer cancel()
	}

	buff := make([]byte, len(b))

	n, err := wc.real.Read(buff)
	if err != nil {
		return n, err
	}

	step := wc.limiter.Burst()
	startChunk := 0
	for ; startChunk < n; startChunk += step {
		if step > n-startChunk {
			step = n - startChunk
		}
		err = wc.limiter.WaitN(ctx, step)
		if err != nil {
			return 0, GetOpTimeoutError(wc.real, "read")
		}
	}
	copy(b, buff[:n])
	return n, nil
}

func (wc *WrappedNetConnection) Write(b []byte) (int, error) {
	if !wc.directions.IsOutbound() {
		return wc.real.Write(b)
	}

	step := wc.limiter.Burst()
	bLen := len(b)

	chunkStart := 0
	total := 0

	ctx := context.Background()
	var cancel context.CancelFunc
	writeDeadLine := wc.getWriteDeadLine()
	if writeDeadLine != nil {
		ctx, cancel = context.WithDeadline(ctx, *writeDeadLine)
		defer cancel()
	}
	for {
		chunkEnd := chunkStart + step
		if chunkEnd > bLen {
			chunkEnd = bLen
		}
		wrb, err := wc.real.Write(b[chunkStart:chunkEnd])
		total += wrb
		if err != nil {
			return total, err
		}

		currentWriteDeadLine := wc.getWriteDeadLine()
		if currentWriteDeadLine != writeDeadLine && currentWriteDeadLine != nil {
			if time.Now().After(*currentWriteDeadLine) {
				return total, GetOpTimeoutError(wc.real, "write")
			}
		}
		err = wc.limiter.WaitN(ctx, wrb)
		if err != nil {
			return total, GetOpTimeoutError(wc.real, "write")
		}

		if chunkEnd >= bLen {
			return total, nil
		}
		chunkStart += wrb
	}
}

func (wc *WrappedNetConnection) setReadDeadLine(t *time.Time) {
	if t.Equal(neverTimeout) {
		t = nil
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&wc.readDeadLine)), unsafe.Pointer(t))
}

func (wc *WrappedNetConnection) getReadDeadLine() *time.Time {
	return (*time.Time)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&wc.readDeadLine))))
}

func (wc *WrappedNetConnection) setWriteDeadLine(t *time.Time) {
	if t.Equal(neverTimeout) {
		t = nil
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&wc.writeDeadLine)), unsafe.Pointer(t))
}

func (wc *WrappedNetConnection) getWriteDeadLine() *time.Time {
	return (*time.Time)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&wc.writeDeadLine))))
}

// Close closes wrapped connection and marks limiter as closed
// can potentially trigger per connection limiters GC, which is run on the background
func (wc *WrappedNetConnection) Close() error {
	wc.limiter.Close()
	return wc.real.Close()
}

func (wc *WrappedNetConnection) LocalAddr() net.Addr {
	return wc.real.LocalAddr()
}

func (wc *WrappedNetConnection) RemoteAddr() net.Addr {
	return wc.real.RemoteAddr()
}

func (wc *WrappedNetConnection) SetDeadline(t time.Time) error {
	wc.setReadDeadLine(&t)
	wc.setWriteDeadLine(&t)
	return wc.real.SetDeadline(t)
}

func (wc *WrappedNetConnection) SetReadDeadline(t time.Time) error {
	wc.setReadDeadLine(&t)
	return wc.real.SetReadDeadline(t)
}

func (wc *WrappedNetConnection) SetWriteDeadline(t time.Time) error {
	wc.setWriteDeadLine(&t)
	return wc.real.SetWriteDeadline(t)
}
