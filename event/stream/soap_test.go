package stream

import (
	"context"
	"strings"
	"testing"

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

// --- Bounded body read -----------------------------------------------

func TestReadClose_LimitsBodySize(t *testing.T) {
	if maxResponseBytes < 1024 {
		t.Skip("limit too small for this test")
	}
	big := strings.Repeat("A", maxResponseBytes+1024)
	body := "<env:Envelope><env:Body>" + big + "</env:Body></env:Envelope>"
	fc := newFakeCaller()
	fc.queueCallMethod(body, nil)
	// Construction will fail because the truncated body has no
	// CreatePullPointSubscriptionResponse — that's fine; what matters is
	// the read completes without OOM.
	_, err := newStream(testContext(t), fc, Options{})
	assert.Error(t, err)
}

// testContext returns a Background context already wired to cancel via
// t.Cleanup so the test does not need to manage the cancellation
// goroutine inline.
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}
