package rabbitmq

import (
	"time"

	b "github.com/jpillora/backoff"
)

func createBackOff() *b.Backoff {
	return &b.Backoff{
		Min:    100 * time.Millisecond,
		Max:    2 * time.Minute,
		Factor: 2,
		Jitter: true,
	}
}
