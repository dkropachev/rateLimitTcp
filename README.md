# rateLimitTcp

Blazingly fast bandwidth rate limiter ;)

## Features
 - Blazingly fast
 - Reconfigurable on the fly
 - Flexible
 - Easy to use

### Blazingly fast

Just fast

### Reconfigurable on the fly

Don't like limits you have ? No problem - reconfiguration is thread-safe out of box.

List of methods: `SetPerConnectionLimitAndBurst`, `SetPerConnectionLimit`, `SetPerConnectionBurst`, `SetGlobalLimit`, `SetGlobalBurst` 

### Flexible

You have access to low level API and objects: `GetGlobalLimiter`, `GetPerConnectionLimiter`

### Easy to use

It can mimic net.Conn API, use `connLimiter.WrappedNetConnection(conn)` to wrap connection and have limits seamlessly applied when you read and/or write 

# Examples

## net
```golang
package main

import (
	"github.com/dkropachev/rateLimitTcp"
	"github.com/stretchr/testify/assert"
	"log"
	"math/rand"
	"net"
	"testing"
)

const (
	HOST = "localhost"
	TYPE = "tcp"
)

func handleIncomingRequest(conn net.Conn) {
	conn.Write([]byte("HELLO"))
}

func main()  {
	listener, _ := net.Listen("tcp", "localhost:9100")
	limiter := rateLimitTcp.NewRateLimiter(1000, 100, 1000, 100, rateLimitTcp.Outbound&rateLimitTcp.Outbound)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		connLimiter := limiter.GetPerConnectionLimiter(conn.RemoteAddr().String())
		if !connLimiter.Allow() {
			// If limit is already reached, drop the connection
			conn.Close()
			continue
		}
		go handleIncomingRequest(connLimiter.WrappedNetConnection(conn))
	}
}

```
