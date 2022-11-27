package testutils

import (
	"math/rand"
	"net"
	"strconv"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func FindFreePort(network, address string) net.Listener {
	var listener net.Listener
	var err error
	for retry := 0; retry < 10; retry++ {
		listener, err = net.Listen(network, address+":"+strconv.Itoa(int(1025+rand.Int31n(60000))))
		if err == nil {
			return listener
		}
	}
	panic(err.Error())
}
