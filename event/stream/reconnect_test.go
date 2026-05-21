package stream

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createPullPointRespAlt mirrors the first fixture but returns a
// different SubscriptionReference Address so a test can prove that
// subsequent pulls hit the recreated endpoint.
const createPullPointRespAlt = `<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
              xmlns:wsa="http://www.w3.org/2005/08/addressing"
              xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
  <env:Body>
    <tev:CreatePullPointSubscriptionResponse>
      <tev:SubscriptionReference>
        <wsa:Address>http://camera.local/onvif/Events/PullSub_2</wsa:Address>
      </tev:SubscriptionReference>
      <tev:CurrentTime>2026-05-21T10:30:10Z</tev:CurrentTime>
      <tev:TerminationTime>2026-05-21T10:31:10Z</tev:TerminationTime>
    </tev:CreatePullPointSubscriptionResponse>
  </env:Body>
</env:Envelope>`

func TestStream_RecreatesSubscriptionAfterRepeatedPullErrors(t *testing.T) {
	fc := newFakeCaller()
	// Initial subscription.
	fc.queueCallMethod(createPullPointResp, nil)
	// Recreated subscription returns a *different* endpoint.
	fc.queueCallMethod(createPullPointRespAlt, nil)

	// First pull fails. With ReconnectAfterFailures=1 this triggers a
	// recreate; subsequent pulls go to PullSub_2 which we'll observe.
	fc.queueSendSoap("", errors.New("transient failure"))
	fc.queueSendSoap(pullMessagesResp(motionMsg("true")), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		DeviceID:               "cam-1",
		PullTimeout:            50 * time.Millisecond,
		ReconnectAfterFailures: 1,
		RetryBackoff:           10 * time.Millisecond,
		InitialTermination:     30 * time.Second, // keep renew quiet
	})
	require.NoError(t, err)
	defer s.Close()

	ev := receive(t, s.Events(), 2*time.Second)
	assert.Equal(t, KindMotion, ev.Kind)

	fc.mu.Lock()
	defer fc.mu.Unlock()
	require.Len(t, fc.callMethodCalls, 2,
		"expected exactly 2 CallMethod calls (initial + recreate)")
	// The PullMessages call that delivered the motion event must
	// target the new endpoint.
	var newEndpointPulls int
	for _, c := range fc.sendSoapCalls {
		if c[0] == "http://camera.local/onvif/Events/PullSub_2" {
			newEndpointPulls++
		}
	}
	assert.GreaterOrEqual(t, newEndpointPulls, 1,
		"expected pulls against the recreated subscription endpoint")
}

func TestStream_BackoffWhenRecreateFails(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	// After the initial successful create, every CallMethod (recreate)
	// and SendSoap (pull) fails. The loop should keep retrying with
	// exponential backoff rather than blocking forever or spinning.
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

	deadline := time.Now().Add(2 * time.Second)
	var calls atomic.Int32
	for time.Now().Before(deadline) {
		fc.mu.Lock()
		calls.Store(int32(len(fc.callMethodCalls)))
		fc.mu.Unlock()
		if calls.Load() >= 4 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, calls.Load(), int32(4),
		"expected stream to retry recreate (>=3 retries on top of the initial create)")
}

func TestStream_ReconnectAfterFailuresDefault(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 3, o.ReconnectAfterFailures)
}

func TestStream_RetryBackoffDefault(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, time.Second, o.RetryBackoff)
}
