package functional_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/dkropachev/rateLimitTcp"

	"golang.org/x/net/nettest"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

func getLimiterHashFromConnection(conn net.Conn) string {
	return conn.RemoteAddr().String()
}

func TestAdditional(t *testing.T) {
	t.Skip("It is failing due to the nettest suite does not tolerate throttling very well")
	limits := []rate.Limit{256, 1024, 4096, 40960}

	for _, limited := range []rateLimitTcp.Direction{rateLimitTcp.NotOperational, rateLimitTcp.Inbound, rateLimitTcp.Outbound, rateLimitTcp.Inbound | rateLimitTcp.Outbound} {
		t.Run(limited.ToString(), func(t *testing.T) {
			for _, globalLimit := range limits {
				t.Run(fmt.Sprintf("Global%v", globalLimit), func(t *testing.T) {
					for _, connLimit := range limits {
						t.Run(fmt.Sprintf("Conn%v", connLimit), func(t *testing.T) {
							test(t, limited, globalLimit, connLimit)
						})
					}
				})
			}
		})
	}
}

func test(t *testing.T, direction rateLimitTcp.Direction, globalLimit, connLimit rate.Limit) {
	mp := func() (c1 net.Conn, c2 net.Conn, stop func(), err error) {
		listener, err := nettest.NewLocalListener("tcp")
		if err != nil {
			return
		}

		var g errgroup.Group

		g.Go(func() error {
			limiter := rateLimitTcp.NewRateLimiter(globalLimit, connLimit, int(globalLimit), int(connLimit))

			conn, err := listener.Accept()
			if err != nil {
				return err
			}

			connLimiter := limiter.GetPerConnectionLimiter(getLimiterHashFromConnection(conn)).Open()
			wrappedConn := connLimiter.
				WrappedNetConnection(conn).
				SetDefaultTimeout(time.Second).
				SetDirections(direction)

			if direction != wrappedConn.GetDirections() {
				return fmt.Errorf("direction does not match: %v, actual: %v", direction.ToString(), wrappedConn.GetDirections().ToString())
			}

			c1 = wrappedConn
			return nil
		})

		g.Go(func() error {
			client, err := net.Dial(listener.Addr().Network(), listener.Addr().String())
			if err != nil {
				return err
			}

			c2 = client
			return nil
		})

		err = g.Wait()

		stop = func() {
			c1.Close()
			c2.Close()
			listener.Close()
		}

		return c1, c2, stop, err
	}

	nettest.TestConn(t, mp)
}
