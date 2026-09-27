package outbox

import "time"

type RetryPolicy struct {
	Base    time.Duration
	Ceiling time.Duration
	Jitter  Jitter
}

func (p RetryPolicy) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := p.Base
	for i := 1; i < attempt; i++ {
		if delay >= p.Ceiling/2 {
			delay = p.Ceiling
			break
		}
		delay *= 2
	}
	if delay > p.Ceiling {
		delay = p.Ceiling
	}
	if p.Jitter != nil && delay < p.Ceiling {
		// Full positive jitter in [0, 20%] while keeping the ceiling absolute.
		jitter := time.Duration(float64(delay) * 0.20 * p.Jitter.Float64())
		delay += jitter
		if delay > p.Ceiling {
			delay = p.Ceiling
		}
	}
	return delay
}
