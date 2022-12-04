package net_test

import (
	"log"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/dkropachev/rateLimitTcp"
	testutils "github.com/dkropachev/rateLimitTcp/tests/utils"
	"github.com/stretchr/testify/assert"
)

const (
	HOST = "localhost"
	TYPE = "tcp"
)

var initMessage = []byte("Connection created!\n")

func getLimiterHashFromConnection(conn net.Conn) string {
	return conn.RemoteAddr().String()
}

func TestNet_functional(t *testing.T) {
	listener := testutils.FindFreePort(TYPE, HOST)
	defer listener.Close()
	go func() {
		limiter := rateLimitTcp.NewRateLimiter(1000, 100, 1000, 100)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connLimiter := limiter.GetPerConnectionLimiter(getLimiterHashFromConnection(conn)).Open()
			go handleIncomingRequest(connLimiter.WrappedNetConnection(conn).SetDirections(rateLimitTcp.Outbound & rateLimitTcp.Inbound).SetDefaultTimeout(time.Second))
		}
	}()
	listener.Addr()
	client, err := net.Dial(listener.Addr().Network(), listener.Addr().String())
	assert.NoError(t, err)

	buffer := make([]byte, 1024)
	n, err := client.Read(buffer)
	assert.NoError(t, err)
	assert.Equal(t, len(initMessage), n)
	assert.Equal(t, initMessage, buffer[:len(initMessage)])
	randomString := []byte(testutils.RandString(20 + rand.Intn(20)))
	n, err = client.Write(randomString)
	assert.NoError(t, err)
	assert.Equal(t, len(randomString), n)
}

func TestNet_limit(t *testing.T) {
	listener := testutils.FindFreePort(TYPE, HOST)
	defer listener.Close()
	go func() {
		limiter := rateLimitTcp.NewRateLimiter(1000, 100, 1000, 100)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connLimiter := limiter.GetPerConnectionLimiter(getLimiterHashFromConnection(conn)).Open()
			go handleIncomingRequest(connLimiter.WrappedNetConnection(conn).SetDirections(rateLimitTcp.Outbound & rateLimitTcp.Inbound).SetDefaultTimeout(time.Second))
		}
	}()
	listener.Addr()
	client, err := net.Dial(listener.Addr().Network(), listener.Addr().String())
	assert.NoError(t, err)

	buffer := make([]byte, 1024)
	n, err := client.Read(buffer)
	assert.NoError(t, err)
	assert.Equal(t, len(initMessage), n)
	assert.Equal(t, initMessage, buffer[:len(initMessage)])
	randomString := []byte(testutils.RandString(20 + rand.Intn(20)))
	n, err = client.Write(randomString)
	assert.NoError(t, err)
	assert.Equal(t, len(randomString), n)
}

func handleIncomingRequest(conn net.Conn) {
	defer func() {
		_ = conn.Close()
	}()

	_, _ = conn.Write(initMessage)
	buffer := make([]byte, 1024)
	_, err := conn.Read(buffer)
	if err != nil {
		log.Fatal(err)
	}
	_, _ = conn.Write(buffer)
}
