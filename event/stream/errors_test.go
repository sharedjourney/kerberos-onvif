package stream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypedErrors_UnwrapAndOp(t *testing.T) {
	inner := errors.New("boom")
	tests := []struct {
		name string
		err  error
		op   Op
	}{
		{"pull", ErrPullFailed{Err: inner}, OpPull},
		{"renew", ErrRenewFailed{Err: inner}, OpRenew},
		{"recreate", ErrRecreateFailed{Err: inner}, OpRecreate},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.True(t, errors.Is(tc.err, inner), "errors.Is should unwrap to inner")
			assert.Contains(t, tc.err.Error(), "boom")

			// Each typed error exposes Op() for branch-without-string-parse.
			if e, ok := tc.err.(interface{ Op() Op }); ok {
				assert.Equal(t, tc.op, e.Op())
			} else {
				t.Fatalf("%T does not expose Op()", tc.err)
			}
		})
	}
}

func TestStream_PullErrorIsTypedErrPullFailed(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.queueSendSoap("", errors.New("transient"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		PullTimeout:        50 * time.Millisecond,
		RetryBackoff:       10 * time.Millisecond,
		InitialTermination: 30 * time.Second,
	})
	require.NoError(t, err)
	defer s.Close()

	select {
	case e := <-s.Errors():
		var pullErr ErrPullFailed
		require.True(t, errors.As(e, &pullErr), "expected ErrPullFailed, got %T: %v", e, e)
		assert.Contains(t, pullErr.Err.Error(), "transient")
	case <-time.After(time.Second):
		t.Fatal("expected ErrPullFailed on Errors channel")
	}
}

func TestStream_RecreateErrorIsTypedErrRecreateFailed(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.mu.Lock()
	fc.defaultCall = fakeResp{err: errors.New("recreate fail")}
	fc.defaultSendSoap = fakeResp{err: errors.New("pull fail")}
	fc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		PullTimeout:            10 * time.Millisecond,
		ReconnectAfterFailures: 1,
		RetryBackoff:           10 * time.Millisecond,
		InitialTermination:     30 * time.Second,
	})
	require.NoError(t, err)
	defer s.Close()

	deadline := time.Now().Add(time.Second)
	var sawRecreate bool
	for time.Now().Before(deadline) && !sawRecreate {
		select {
		case e := <-s.Errors():
			var rec ErrRecreateFailed
			if errors.As(e, &rec) {
				sawRecreate = true
				assert.Contains(t, rec.Err.Error(), "recreate fail")
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	assert.True(t, sawRecreate, "expected at least one ErrRecreateFailed on Errors")
}

func TestStream_AfterReconnectFlagSetOnFirstPostRecreateBatch(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	// Second create is the recreate.
	fc.queueCallMethod(createPullPointRespAlt, nil)

	// First pull fails -> triggers recreate with ReconnectAfterFailures=1.
	fc.queueSendSoap("", errors.New("transient"))
	// First pull after recreate: a Changed motion event. The flag
	// should be true, and should clear (because we received a
	// non-Initialized event).
	fc.queueSendSoap(pullMessagesResp(motionMsg("true")), nil)
	// Second pull after recreate: another motion event. Flag should
	// now be false.
	fc.queueSendSoap(pullMessagesResp(motionMsg("false")), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		PullTimeout:            50 * time.Millisecond,
		ReconnectAfterFailures: 1,
		RetryBackoff:           10 * time.Millisecond,
		InitialTermination:     30 * time.Second,
	})
	require.NoError(t, err)
	defer s.Close()

	ev1 := receive(t, s.Events(), 2*time.Second)
	assert.True(t, ev1.AfterReconnect, "first event after recreate must carry AfterReconnect=true")
	assert.Equal(t, StateActive, ev1.State)

	ev2 := receive(t, s.Events(), 2*time.Second)
	assert.False(t, ev2.AfterReconnect, "subsequent events should not carry AfterReconnect")
	assert.Equal(t, StateInactive, ev2.State)
}
