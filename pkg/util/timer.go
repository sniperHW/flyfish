package util

import (
	"time"
)

func OnceTimer(timeout time.Duration, fn func()) {
	go func() {
		<-time.After(timeout)
		fn()
	}()
}
