package rateLimitTcp

import "net"

type TimeoutError struct{}

func (e *TimeoutError) Error() string   { return "i/o timeout" }
func (e *TimeoutError) Timeout() bool   { return true }
func (e *TimeoutError) Temporary() bool { return true }

func GetOpTimeoutError(conn net.Conn, op string) *net.OpError {
	laddr := conn.LocalAddr()
	raddr := conn.RemoteAddr()
	return &net.OpError{Op: "write", Net: laddr.Network(), Source: laddr, Addr: raddr, Err: &TimeoutError{}}
}
