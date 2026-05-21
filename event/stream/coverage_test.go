package stream

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kerberos-io/onvif/event"
	"github.com/kerberos-io/onvif/xsd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Close surfaces unsubscribe error --------------------------------

func TestClose_ReturnsUnsubscribeError(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	// Default empty pulls keep the loop running. Override default
	// SendSoap to fail so Close's Unsubscribe also fails.
	fc.mu.Lock()
	fc.defaultSendSoap = fakeResp{err: errors.New("simulated transport failure")}
	fc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{InitialTermination: 30 * time.Second})
	require.NoError(t, err)

	err = s.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsubscribe pull point")
	assert.Contains(t, err.Error(), "simulated transport failure")
}

// --- NewStream against already-cancelled context ----------------------

func TestNewStream_CtxAlreadyCancelled(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before NewStream

	s, err := newStream(ctx, fc, Options{InitialTermination: 30 * time.Second})
	// Create-pull-point doesn't currently consult ctx (it uses caller
	// directly), so construction succeeds and the run goroutine exits
	// immediately. Close must still work cleanly.
	require.NoError(t, err)
	require.NotNil(t, s)

	// Events channel must close promptly because the goroutine exits.
	select {
	case _, ok := <-s.Events():
		assert.False(t, ok, "events channel should be closed when ctx is pre-cancelled")
	case <-time.After(time.Second):
		t.Fatal("events channel was not closed within 1s")
	}
	_ = s.Close()
}

// --- DisableReconnect honours the opt-out ----------------------------

func TestStream_DisableReconnectKeepsRetryingOriginalEndpoint(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	// All pulls fail; default SendSoap stays as empty-pull (success)
	// only if the fake's queue exhausts — we override default to a
	// failure so EVERY pull errors.
	fc.mu.Lock()
	fc.defaultSendSoap = fakeResp{err: errors.New("pull fail")}
	fc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		PullTimeout:        10 * time.Millisecond,
		RetryBackoff:       10 * time.Millisecond,
		InitialTermination: 30 * time.Second,
		DisableReconnect:   true,
	})
	require.NoError(t, err)
	defer s.Close()

	// Let the loop spin for a bit, then assert no second CallMethod
	// (recreate would invoke CallMethod, which we are watching).
	time.Sleep(200 * time.Millisecond)
	fc.mu.Lock()
	calls := len(fc.callMethodCalls)
	fc.mu.Unlock()
	assert.Equal(t, 1, calls, "DisableReconnect must prevent recreate; got %d CallMethod calls", calls)
}

// --- Recreate resets failures+backoff on success ---------------------

func TestStream_RecreateResetsFailuresAndBackoffOnSuccess(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.queueCallMethod(createPullPointRespAlt, nil)
	// Pull fails once -> triggers recreate -> recreate succeeds ->
	// next pull succeeds. After that we should NOT see another
	// recreate (failures was reset). Provide enough successful empty
	// pulls.
	fc.queueSendSoap("", errors.New("first failure"))
	// Subsequent pulls succeed via default empty pull.

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

	time.Sleep(200 * time.Millisecond)
	fc.mu.Lock()
	calls := len(fc.callMethodCalls)
	fc.mu.Unlock()
	assert.Equal(t, 2, calls,
		"after one failure + successful recreate, no further recreates expected; got %d", calls)
}

// --- pullPointMu under race ------------------------------------------

func TestStream_PullPointMutationVisibleToRenewLoopUnderRace(t *testing.T) {
	// Drives the pullPoint write-by-pullLoop / read-by-renewLoop race
	// so -race actually exercises the mutex critical sections. With
	// short termination and quick recreate, renew is firing alongside
	// the recreate write.
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	// Queue a stream of alt-response recreates so each retry installs
	// a new pullPoint.
	for i := 0; i < 50; i++ {
		fc.queueCallMethod(createPullPointRespAlt, nil)
	}
	// Default empty pulls.
	// Force pull errors so reconnect path fires repeatedly: override
	// default and queue mostly-failing pulls.
	fc.mu.Lock()
	fc.defaultSendSoap = fakeResp{err: errors.New("recurring pull fail")}
	fc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		PullTimeout:            5 * time.Millisecond,
		ReconnectAfterFailures: 1,
		RetryBackoff:           1 * time.Millisecond,
		InitialTermination:     20 * time.Millisecond,
		RenewMargin:            2 * time.Millisecond,
	})
	require.NoError(t, err)
	defer s.Close()

	// Spin for ~300ms; the race detector will fire if either
	// pullPointMu critical section is broken. We don't assert on
	// content here — the value is the -race signal.
	time.Sleep(300 * time.Millisecond)
}

// --- fakeCaller self-test --------------------------------------------

func TestFakeCaller_QueueThenDefaultFallback(t *testing.T) {
	fc := newFakeCaller()
	fc.queueSendSoap("first", nil)
	fc.queueSendSoap("second", nil)
	// Default already set to an empty pull response.

	r1, err := fc.SendSoap("ep", "body")
	require.NoError(t, err)
	b1 := make([]byte, 10)
	n, _ := r1.Body.Read(b1)
	assert.Equal(t, "first", string(b1[:n]))

	r2, _ := fc.SendSoap("ep", "body")
	b2 := make([]byte, 10)
	n, _ = r2.Body.Read(b2)
	assert.Equal(t, "second", string(b2[:n]))

	// Queue is exhausted; default kicks in.
	r3, err := fc.SendSoap("ep", "body")
	require.NoError(t, err)
	require.NotNil(t, r3)
	b3 := make([]byte, 2048)
	n, _ = r3.Body.Read(b3)
	assert.Contains(t, string(b3[:n]), "PullMessagesResponse",
		"default SendSoap should be an empty PullMessagesResponse envelope")
}

// --- Decoder coverage gaps -------------------------------------------

func TestDecode_PropertyOperationIsCaseSensitive(t *testing.T) {
	// Per WS-Notification §3.3 PropertyOperation values are
	// 'Initialized' / 'Changed' / 'Deleted'. Lowercased forms in the
	// wild are malformed and should fall through to PropertyUnknown.
	in := msg("tns1:VideoSource/MotionAlarm", "changed", "", nil, nil)
	ev := decode(in, "dev", time.Now())
	assert.Equal(t, PropertyUnknown, ev.Operation)
}

func TestDecode_StateValueTrimsWhitespace(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  State
	}{
		{"leading_trailing", "  true  ", StateActive},
		{"tab_newline", "\ttrue\n", StateActive},
		{"only_spaces", "   ", StateUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := msg("tns1:VideoSource/MotionAlarm", "Changed", "",
				nil, map[string]string{"State": tc.value})
			ev := decode(in, "dev", time.Now())
			assert.Equal(t, tc.want, ev.State)
		})
	}
}

func TestDecode_SimpleItemEmptyValueIsUnknownState(t *testing.T) {
	in := msg("tns1:VideoSource/MotionAlarm", "Changed", "",
		nil, map[string]string{"State": ""})
	ev := decode(in, "dev", time.Now())
	assert.Equal(t, StateUnknown, ev.State)
	// Empty value still preserved in the Data map.
	v, ok := ev.Data["State"]
	assert.True(t, ok)
	assert.Equal(t, "", v)
}

func TestDecode_DeviceTimeAdditionalLayouts(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want time.Time
	}{
		{"compact_offset", "2026-05-21T12:30:00+0200", time.Date(2026, 5, 21, 10, 30, 0, 0, time.UTC)},
		{"compact_offset_subsec", "2026-05-21T12:30:00.500+0200", time.Date(2026, 5, 21, 10, 30, 0, 500_000_000, time.UTC)},
		{"naked_no_tz", "2026-05-21T10:30:00", time.Date(2026, 5, 21, 10, 30, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := msg("tns1:VideoSource/MotionAlarm", "Changed", tc.in, nil, nil)
			ev := decode(in, "dev", time.Now())
			assert.True(t, ev.DeviceTime.Equal(tc.want),
				"input=%q got=%v want=%v", tc.in, ev.DeviceTime, tc.want)
		})
	}
}

// --- extractState deterministic order with explicit slice ------------

func TestExtractState_FirstBooleanLikeWins(t *testing.T) {
	// Verifies the documented behaviour: when multiple Data items have
	// boolean-like values, the first by slice order wins.
	in := msg("tns1:VideoSource/MotionAlarm", "Changed", "", nil, nil)
	in.Message.Message.Data.SimpleItem = simpleItemsFromPairs([]pair{
		{"ObjectId", "42"},
		{"State", "true"},
		{"Trailer", "false"},
	})
	ev := decode(in, "dev", time.Now())
	assert.Equal(t, StateActive, ev.State,
		"first boolean-like value (State=true) must win, not Trailer=false")
}

type pair struct{ k, v string }

func simpleItemsFromPairs(pairs []pair) []event.SimpleItem {
	out := make([]event.SimpleItem, len(pairs))
	for i, p := range pairs {
		out[i] = event.SimpleItem{
			Name:  xsd.AnyType(p.k),
			Value: xsd.AnyType(p.v),
		}
	}
	return out
}

// --- ensure the new layouts don't accept unrelated junk --------------

func TestDecode_DeviceTimeStillRejectsNonsense(t *testing.T) {
	for _, s := range []string{"hello", "2026-13-45T99:99:99", strings.Repeat("9", 50)} {
		in := msg("tns1:VideoSource/MotionAlarm", "Changed", s, nil, nil)
		ev := decode(in, "dev", time.Now())
		assert.True(t, ev.DeviceTime.IsZero(), "input=%q should yield zero, got %v", s, ev.DeviceTime)
	}
}
