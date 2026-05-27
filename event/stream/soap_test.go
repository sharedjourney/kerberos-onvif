package stream

import (
	"context"
	"errors"
	"io"
	"net/http"
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

// --- Error enrichment from SOAP response bodies ----------------------

func fakeResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusBadRequest,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestEnrichSOAPErr_NilErrReturnsNil(t *testing.T) {
	assert.NoError(t, enrichSOAPErr(fakeResponse("anything"), nil))
}

func TestEnrichSOAPErr_NilRespPreservesOriginal(t *testing.T) {
	orig := errors.New("transport boom")
	got := enrichSOAPErr(nil, orig)
	assert.ErrorIs(t, got, orig)
	assert.Equal(t, orig.Error(), got.Error(), "no body, no extra context to add")
}

func TestEnrichSOAPErr_SOAP11FaultStringAppearsInError(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
  <env:Body><env:Fault><faultstring>not authorized</faultstring></env:Fault></env:Body>
</env:Envelope>`
	got := enrichSOAPErr(fakeResponse(body), errors.New("400 Bad Request"))
	require.Error(t, got)
	assert.Contains(t, got.Error(), "400 Bad Request")
	assert.Contains(t, got.Error(), "not authorized")
}

func TestEnrichSOAPErr_SOAP12ReasonAppearsInError(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Body><env:Fault>
    <env:Code><env:Value>env:Sender</env:Value></env:Code>
    <env:Reason><env:Text xml:lang="en">Subscription has expired</env:Text></env:Reason>
  </env:Fault></env:Body>
</env:Envelope>`
	got := enrichSOAPErr(fakeResponse(body), errors.New("400 Bad Request"))
	require.Error(t, got)
	assert.Contains(t, got.Error(), "Subscription has expired")
}

// Pins the AXIS case: a Fault with populated Subcode but an empty
// <Reason><Text/></Reason>. Without subcode fallback, the only signal
// the operator sees is "400 Bad Request".
func TestEnrichSOAPErr_EmptyReasonFallsBackToSubcode(t *testing.T) {
	body := `<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope" xmlns:ter="http://www.onvif.org/ver10/error">
  <SOAP-ENV:Body><SOAP-ENV:Fault>
    <SOAP-ENV:Code>
      <SOAP-ENV:Value>SOAP-ENV:Sender</SOAP-ENV:Value>
      <SOAP-ENV:Subcode><SOAP-ENV:Value>ter:InvalidArgs</SOAP-ENV:Value></SOAP-ENV:Subcode>
    </SOAP-ENV:Code>
    <SOAP-ENV:Reason><SOAP-ENV:Text xml:lang="en"/></SOAP-ENV:Reason>
  </SOAP-ENV:Fault></SOAP-ENV:Body>
</SOAP-ENV:Envelope>`
	got := enrichSOAPErr(fakeResponse(body), errors.New("Post with digest error: 400: 400 Bad Request"))
	require.Error(t, got)
	assert.Contains(t, got.Error(), "ter:InvalidArgs",
		"AXIS-style empty-Reason Faults must surface their Subcode")
}

func TestEnrichSOAPErr_NonFaultBodyIncludesExcerpt(t *testing.T) {
	body := `<html><body>404 Not Found — /onvif/services missing</body></html>`
	got := enrichSOAPErr(fakeResponse(body), errors.New("404 Not Found"))
	require.Error(t, got)
	assert.Contains(t, got.Error(), "/onvif/services missing")
}

func TestEnrichSOAPErr_LargeNonFaultBodyTruncated(t *testing.T) {
	// A misbehaving camera could stream a multi-megabyte body. The
	// helper must cap the excerpt so a wedged camera does not flood
	// logs.
	body := strings.Repeat("X", 8192)
	got := enrichSOAPErr(fakeResponse(body), errors.New("500"))
	require.Error(t, got)
	assert.Less(t, len(got.Error()), 2048,
		"enriched error must stay log-line sized even on huge bodies")
}

func TestEnrichSOAPErr_PreservesOriginalForErrorsIs(t *testing.T) {
	// Callers wrap pull/renew/recreate errors with errors.As in
	// logStreamError; enrichment must keep the original wrappable.
	orig := errors.New("sentinel")
	got := enrichSOAPErr(fakeResponse(`<env:Fault><faultstring>x</faultstring></env:Fault>`), orig)
	assert.ErrorIs(t, got, orig)
}

// --- Subcode extraction ----------------------------------------------

func TestExtractSOAPSubcode_Present(t *testing.T) {
	body := `<SOAP-ENV:Code>
  <SOAP-ENV:Value>SOAP-ENV:Sender</SOAP-ENV:Value>
  <SOAP-ENV:Subcode><SOAP-ENV:Value>ter:InvalidArgs</SOAP-ENV:Value></SOAP-ENV:Subcode>
</SOAP-ENV:Code>`
	assert.Equal(t, "ter:InvalidArgs", extractSOAPSubcode(body))
}

func TestExtractSOAPSubcode_Absent(t *testing.T) {
	assert.Empty(t, extractSOAPSubcode(`<env:Code><env:Value>env:Sender</env:Value></env:Code>`))
}

// --- Wiring: each SOAP call site routes errors through enrichSOAPErr -

const faultBody = `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Body><env:Fault>
    <env:Code><env:Value>env:Sender</env:Value></env:Code>
    <env:Reason><env:Text xml:lang="en">camera-specific complaint</env:Text></env:Reason>
  </env:Fault></env:Body>
</env:Envelope>`

func TestCreatePullPoint_EnrichesTransportErrWithFaultReason(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(faultBody, errors.New("400 Bad Request"))
	_, err := createPullPoint(fc, defaultOptions())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "camera-specific complaint",
		"createPullPoint must enrich transport errors with the camera's SOAP fault")
}

func TestPullMessages_EnrichesTransportErrWithFaultReason(t *testing.T) {
	fc := newFakeCaller()
	fc.queueSendSoap(faultBody, errors.New("400 Bad Request"))
	_, err := pullMessages(fc, subscriptionRef{Address: "http://camera/sub"}, defaultOptions())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "camera-specific complaint",
		"pullMessages must enrich transport errors with the camera's SOAP fault")
}

func TestUnsubscribePullPoint_EnrichesTransportErrWithFaultReason(t *testing.T) {
	fc := newFakeCaller()
	fc.queueSendSoap(faultBody, errors.New("400 Bad Request"))
	err := unsubscribePullPoint(fc, subscriptionRef{Address: "http://camera/sub"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "camera-specific complaint",
		"unsubscribePullPoint must enrich transport errors with the camera's SOAP fault")
}

// --- ReferenceParameters extraction (WS-Addressing 1.0 §3.1) ---------
//
// AXIS encodes the subscription identity in <wsa:ReferenceParameters>
// inside CreatePullPointSubscriptionResponse rather than in the URL
// itself. Subsequent PullMessages/Renew/Unsubscribe MUST echo those
// elements verbatim into the SOAP Header, or the camera responds with
// ter:InvalidArgs. The auto-generated event.ReferenceParametersType is
// an empty struct (drops children), so we extract the raw inner XML.

func TestExtractReferenceParameters_AXISShape(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
                      xmlns:wsa5="http://www.w3.org/2005/08/addressing"
                      xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
  <env:Body><tev:CreatePullPointSubscriptionResponse>
    <tev:SubscriptionReference>
      <wsa5:Address>http://192.168.1.10/onvif/services</wsa5:Address>
      <wsa5:ReferenceParameters>
        <dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event">297</dom0:SubscriptionId>
      </wsa5:ReferenceParameters>
    </tev:SubscriptionReference>
  </tev:CreatePullPointSubscriptionResponse></env:Body>
</env:Envelope>`
	got := extractReferenceParameters(body)
	assert.Contains(t, got, "SubscriptionId")
	assert.Contains(t, got, "297")
	assert.Contains(t, got, `xmlns:dom0="http://www.axis.com/2009/event"`,
		"namespace declaration on the SubscriptionId child must survive extraction")
}

func TestExtractReferenceParameters_AbsentReturnsEmpty(t *testing.T) {
	// Geovision/Hikvision-style: Address only, no ReferenceParameters.
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
                      xmlns:wsa="http://www.w3.org/2005/08/addressing">
  <env:Body><tev:CreatePullPointSubscriptionResponse xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
    <tev:SubscriptionReference>
      <wsa:Address>http://camera/onvif/Events/Sub_1</wsa:Address>
    </tev:SubscriptionReference>
  </tev:CreatePullPointSubscriptionResponse></env:Body>
</env:Envelope>`
	assert.Empty(t, extractReferenceParameters(body))
}

func TestExtractReferenceParameters_EmptyBodyReturnsEmpty(t *testing.T) {
	assert.Empty(t, extractReferenceParameters(""))
}

// --- createPullPoint returns both address and ref params -------------

func TestCreatePullPoint_ReturnsRefParamsAlongsideAddress(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
                      xmlns:wsa5="http://www.w3.org/2005/08/addressing"
                      xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
  <env:Body><tev:CreatePullPointSubscriptionResponse>
    <tev:SubscriptionReference>
      <wsa5:Address>http://192.168.1.10/onvif/services</wsa5:Address>
      <wsa5:ReferenceParameters>
        <dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event">297</dom0:SubscriptionId>
      </wsa5:ReferenceParameters>
    </tev:SubscriptionReference>
  </tev:CreatePullPointSubscriptionResponse></env:Body>
</env:Envelope>`
	fc := newFakeCaller()
	fc.queueCallMethod(body, nil)
	ref, err := createPullPoint(fc, defaultOptions())
	require.NoError(t, err)
	assert.Equal(t, "http://192.168.1.10/onvif/services", ref.Address)
	assert.Contains(t, ref.RefParamsXML, "SubscriptionId")
	assert.Contains(t, ref.RefParamsXML, "297")
}

func TestCreatePullPoint_VendorWithoutRefParams_RefParamsEmpty(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	ref, err := createPullPoint(fc, defaultOptions())
	require.NoError(t, err)
	assert.NotEmpty(t, ref.Address)
	assert.Empty(t, ref.RefParamsXML)
}

// --- Reference-parameter echoing in subscription-scoped calls --------
//
// WS-Addressing 1.0 §3.1 requires each <wsa:ReferenceParameters> child
// to be echoed as a SOAP Header block carrying wsa:IsReferenceParameter
// ="true". AXIS rejects PullMessages with ter:InvalidArgs when this is
// absent.

func TestPullMessages_EchoesRefParamsWithIsReferenceParameterAttribute(t *testing.T) {
	ref := subscriptionRef{
		Address:      "http://192.168.1.10/onvif/services",
		RefParamsXML: `<dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event">297</dom0:SubscriptionId>`,
	}
	fc := newFakeCaller()
	_, err := pullMessages(fc, ref, defaultOptions())
	require.NoError(t, err)
	require.Len(t, fc.sendSoapHeaders, 1)
	hdr := fc.sendSoapHeaders[0]
	assert.Contains(t, hdr, "SubscriptionId", "ref param element must be echoed")
	assert.Contains(t, hdr, "297", "ref param value must be echoed")
	assert.Contains(t, hdr, `IsReferenceParameter="true"`,
		"WS-Addressing 1.0 §3.1 requires the attribute on each echoed element")
}

func TestPullMessages_NoRefParams_HeaderEmpty(t *testing.T) {
	ref := subscriptionRef{Address: "http://camera/sub", RefParamsXML: ""}
	fc := newFakeCaller()
	_, err := pullMessages(fc, ref, defaultOptions())
	require.NoError(t, err)
	require.Len(t, fc.sendSoapHeaders, 1)
	assert.Empty(t, fc.sendSoapHeaders[0], "vendors without ref params get no extra header")
}

func TestPullMessages_PostsToAddressFromRef(t *testing.T) {
	ref := subscriptionRef{Address: "http://camera/specific-sub-endpoint", RefParamsXML: ""}
	fc := newFakeCaller()
	_, err := pullMessages(fc, ref, defaultOptions())
	require.NoError(t, err)
	require.NotEmpty(t, fc.sendSoapCalls)
	assert.Equal(t, "http://camera/specific-sub-endpoint", fc.sendSoapCalls[0][0])
}

// --- Building the header XML from raw ref params ----------------------

func TestBuildRefParamsHeader_AddsIsReferenceParameter(t *testing.T) {
	raw := `<dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event">297</dom0:SubscriptionId>`
	got, err := buildRefParamsHeader(raw)
	require.NoError(t, err)
	assert.Contains(t, got, "SubscriptionId")
	assert.Contains(t, got, "297")
	assert.Contains(t, got, `xmlns:dom0="http://www.axis.com/2009/event"`,
		"original namespace declaration must survive")
	assert.Contains(t, got, `IsReferenceParameter="true"`)
}

func TestBuildRefParamsHeader_MultipleTopLevelChildren(t *testing.T) {
	raw := `<a:Foo xmlns:a="ns/a">1</a:Foo><b:Bar xmlns:b="ns/b">2</b:Bar>`
	got, err := buildRefParamsHeader(raw)
	require.NoError(t, err)
	assert.Equal(t, 2, strings.Count(got, `IsReferenceParameter="true"`),
		"attribute must be added to every top-level child, not just the first")
	assert.Contains(t, got, "Foo")
	assert.Contains(t, got, "Bar")
}

func TestBuildRefParamsHeader_EmptyInputReturnsEmpty(t *testing.T) {
	got, err := buildRefParamsHeader("")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestUnsubscribePullPoint_EchoesRefParamsWithIsReferenceParameter(t *testing.T) {
	ref := subscriptionRef{
		Address:      "http://192.168.1.10/onvif/services",
		RefParamsXML: `<dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event">297</dom0:SubscriptionId>`,
	}
	fc := newFakeCaller()
	require.NoError(t, unsubscribePullPoint(fc, ref))
	require.Len(t, fc.sendSoapHeaders, 1)
	hdr := fc.sendSoapHeaders[0]
	assert.Contains(t, hdr, "SubscriptionId")
	assert.Contains(t, hdr, `IsReferenceParameter="true"`)
}

func TestUnsubscribePullPoint_EmptyAddressIsNoOp(t *testing.T) {
	fc := newFakeCaller()
	require.NoError(t, unsubscribePullPoint(fc, subscriptionRef{}))
	assert.Empty(t, fc.sendSoapCalls, "no SOAP call should happen when there is no subscription endpoint")
}
