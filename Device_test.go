package onvif

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDevice_SetDeviceInfoFromScopes(t *testing.T) {
	const (
		name     = "DeviceName"
		hardware = "M9000"
	)
	scopes := []string{
		"onvif://www.onvif.org/Profile/Streaming",
		"onvif://www.onvif.org/SomethingElse/value",
		"onvif://www.onvif.org/name/" + name,
		"onvif://www.onvif.org/hardware/" + hardware,
	}
	device := Device{}
	device.SetDeviceInfoFromScopes(scopes)
	assert.Equal(t, device.info.Name, name)
	assert.Equal(t, device.info.Model, hardware)
}

// TestDevice_SendSoapWithHeader_InjectsHeaderXML verifies that the
// supplied header XML lands inside the SOAP <Header> element of the
// outgoing request. AXIS-style WS-Addressing reference parameter
// echoing depends on this — without it the camera returns
// ter:InvalidArgs on every PullMessages.
func TestDevice_SendSoapWithHeader_InjectsHeaderXML(t *testing.T) {
	const headerXML = `<dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event" wsa:IsReferenceParameter="true">297</dom0:SubscriptionId>`
	const bodyXML = `<tev:PullMessages xmlns:tev="http://www.onvif.org/ver10/events/wsdl"><tev:Timeout>PT5S</tev:Timeout><tev:MessageLimit>32</tev:MessageLimit></tev:PullMessages>`

	var captured string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		captured = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dev := Device{
		params: DeviceParams{
			Xaddr:      strings.TrimPrefix(srv.URL, "http://"),
			HttpClient: srv.Client(),
		},
	}
	resp, err := dev.SendSoapWithHeader(srv.URL, bodyXML, headerXML)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}

	headerStart := strings.Index(captured, "Header>")
	headerEnd := strings.Index(captured, "</")
	require.NotEqual(t, -1, headerStart, "envelope must contain <Header>; got: %s", captured)
	assert.Greater(t, headerEnd, headerStart, "envelope must close the header")

	headerSlice := captured[headerStart:strings.Index(captured, "Body>")]
	assert.Contains(t, headerSlice, "SubscriptionId",
		"injected header element must land inside SOAP <Header>")
	assert.Contains(t, headerSlice, "297")

	bodySlice := captured[strings.Index(captured, "Body>"):]
	assert.Contains(t, bodySlice, "PullMessages",
		"body content must land inside SOAP <Body>")
}
