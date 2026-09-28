package engine

import "sync/atomic"

var idCounter int64

func id() string {
	n := atomic.AddInt64(&idCounter, 1)
	return "auto-id-" + itoaTest(n)
}

func itoaTest(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
