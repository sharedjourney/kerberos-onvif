package stream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJitter_StaysWithinFraction(t *testing.T) {
	const base = time.Second
	low := time.Duration(float64(base) * (1 - jitterFraction))
	high := time.Duration(float64(base) * (1 + jitterFraction))
	for i := 0; i < 200; i++ {
		got := jitter(base)
		assert.GreaterOrEqual(t, got, low, "iteration %d", i)
		assert.LessOrEqual(t, got, high, "iteration %d", i)
	}
}

func TestJitter_ZeroAndNegativeReturnPositive(t *testing.T) {
	assert.Greater(t, jitter(0), time.Duration(0))
	assert.Greater(t, jitter(-time.Second), time.Duration(0))
}

func TestJitter_VariesAcrossCalls(t *testing.T) {
	// Sanity check that we're not returning a constant. Vanishingly
	// unlikely to flake (probability ~ (1/uint64-space)^9).
	first := jitter(time.Second)
	allEqual := true
	for i := 0; i < 10; i++ {
		if jitter(time.Second) != first {
			allEqual = false
			break
		}
	}
	assert.False(t, allEqual, "jitter is producing a constant; rand seed not working")
}

func TestMaxRecreateBackoff_Is5Minutes(t *testing.T) {
	// Document the policy choice in a test so a future maintainer
	// changing this notices.
	assert.Equal(t, 5*time.Minute, maxRecreateBackoff)
}
