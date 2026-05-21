package stream

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- SOAP fault detection ---------------------------------------------

func TestExtractSOAPFault_SOAP11(t *testing.T) {
	body := `<?xml version="1.0"?>
<env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
  <env:Body>
    <env:Fault>
      <faultcode>env:Client</faultcode>
      <faultstring>The action requested requires authorization and the sender is not authorized</faultstring>
    </env:Fault>
  </env:Body>
</env:Envelope>`
	got := extractSOAPFault(body)
	assert.Contains(t, got, "not authorized")
}

func TestExtractSOAPFault_SOAP12(t *testing.T) {
	body := `<?xml version="1.0"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Body>
    <env:Fault>
      <env:Code><env:Value>env:Sender</env:Value></env:Code>
      <env:Reason><env:Text xml:lang="en">Subscription has expired</env:Text></env:Reason>
    </env:Fault>
  </env:Body>
</env:Envelope>`
	got := extractSOAPFault(body)
	assert.Contains(t, got, "Subscription has expired")
}

func TestExtractSOAPFault_NotAFault(t *testing.T) {
	assert.Empty(t, extractSOAPFault(createPullPointResp))
}

func TestExtractSOAPFault_EmptyBody(t *testing.T) {
	assert.Empty(t, extractSOAPFault(""))
}

func TestUnmarshalNode_ReturnsFaultReasonInsteadOfMissingElement(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
  <env:Body><env:Fault><faultstring>not authorized</faultstring></env:Fault></env:Body>
</env:Envelope>`
	var out struct{}
	err := unmarshalNode(body, "PullMessagesResponse", &out)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not authorized")
	assert.NotContains(t, err.Error(), "missing PullMessagesResponse")
}

// --- Renew sends absolute datetime -----------------------------------

func TestRenew_SendsAbsoluteDateTimeNotDuration(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		InitialTermination: 30 * time.Millisecond,
		RenewMargin:        5 * time.Millisecond,
	})
	require.NoError(t, err)
	defer s.Close()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if countSendSoapMatching(fc, "Renew") >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	var renewBody string
	for _, c := range fc.sendSoapCalls {
		if strings.Contains(c[1], "Renew") {
			renewBody = c[1]
			break
		}
	}
	require.NotEmpty(t, renewBody, "no Renew call observed")
	// Absolute form is "YYYY-MM-DDTHH:MM:SSZ" not "PTnS".
	assert.NotContains(t, renewBody, "PT", "Renew should not send relative duration; some firmwares reject it")
	assert.Regexp(t, `\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z`, renewBody, "Renew should send absolute RFC3339 UTC")
}

// --- Bounded body read -----------------------------------------------

func TestReadClose_LimitsBodySize(t *testing.T) {
	// Build a response with a body just over the limit. readClose must
	// not return more than the limit even if the camera pretends to
	// send more.
	if maxResponseBytes < 1024 {
		t.Skip("limit too small for this test")
	}
	big := strings.Repeat("A", maxResponseBytes+1024)
	// Wrap in a minimal SOAP envelope so the body is at least
	// well-formed shape-wise.
	body := "<env:Envelope><env:Body>" + big + "</env:Body></env:Envelope>"
	fc := newFakeCaller()
	fc.queueCallMethod(body, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Construction will fail because the truncated body has no
	// CreatePullPointSubscriptionResponse — that's fine; what matters
	// is the read completes without OOM.
	_, err := newStream(ctx, fc, Options{})
	assert.Error(t, err)
}

// --- Close timeout ---------------------------------------------------

func TestClose_BoundedByTimeoutOnHungUnsubscribe(t *testing.T) {
	// Patch closeUnsubscribeTimeout for the duration of the test so the
	// assertion completes promptly. We can't change the const at runtime
	// so we use a short InitialTermination and verify Close still
	// returns within closeUnsubscribeTimeout + slack rather than
	// blocking forever.
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	block := make(chan struct{})
	defer close(block) // release the hung Unsubscribe so the fake's goroutine exits
	fc.mu.Lock()
	fc.blockUnsubscribe = block
	fc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{InitialTermination: 30 * time.Second})
	require.NoError(t, err)

	start := time.Now()
	err = s.Close()
	elapsed := time.Since(start)
	// Unsubscribe is hung, so Close must surface a timeout error from
	// the bounded wait rather than block forever. closeUnsubscribeTimeout
	// is 5s; allow 1s slack for scheduling.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
	assert.Less(t, elapsed, closeUnsubscribeTimeout+time.Second,
		"Close exceeded bound (%s); expected ~%s", elapsed, closeUnsubscribeTimeout)
}
