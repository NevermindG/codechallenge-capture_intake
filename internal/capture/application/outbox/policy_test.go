package outbox

import (
	"testing"
	"time"
)

type fixedJitter struct{ value float64 }

func (j fixedJitter) Float64() float64 { return j.value }

func TestRetryPolicyIsExponentialWithJitterAndCeiling(t *testing.T) {
	p := RetryPolicy{Base: time.Second, Ceiling: 5 * time.Second, Jitter: fixedJitter{value: 0.5}}
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1100 * time.Millisecond},
		{2, 2200 * time.Millisecond},
		{3, 4400 * time.Millisecond},
		{4, 5 * time.Second},
	}
	for _, tc := range cases {
		if got := p.Next(tc.attempt); got != tc.want {
			t.Fatalf("attempt %d: got %v want %v", tc.attempt, got, tc.want)
		}
	}
}
