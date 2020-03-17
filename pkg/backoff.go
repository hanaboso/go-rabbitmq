package pkg

import (
	"time"

	"github.com/jpillora/backoff"
)

func defaultBackOff() *backoff.Backoff {
	return createBackOff(100*time.Millisecond, 2*time.Minute, 2, true)
}

func createBackOff(min time.Duration, max time.Duration, factor float64, jitter bool) *backoff.Backoff {
	return &backoff.Backoff{
		Min:    min,
		Max:    max,
		Factor: factor,
		Jitter: jitter,
	}
}
