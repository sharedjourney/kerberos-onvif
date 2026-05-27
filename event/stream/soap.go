package stream

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/kerberos-io/onvif/event"
	"github.com/kerberos-io/onvif/xsd"
)

// maxResponseBytes caps SOAP response buffering. ONVIF PullMessages
// bodies are normally <100KB even with dense analytics payloads;
// 10 MiB is comfortably above legitimate traffic while keeping a
// hostile or buggy camera from OOMing the process.
const maxResponseBytes = 10 << 20

func createPullPoint(c caller, opts Options) (string, error) {
	term := xsd.String(durationToXSD(opts.InitialTermination))
	req := event.CreatePullPointSubscription{InitialTerminationTime: &term}
	if opts.RawTopicFilter != "" {
		req.Filter = &event.FilterType{
			TopicExpression: &event.TopicExpressionType{
				Dialect:    xsd.String("http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet"),
				TopicKinds: xsd.String(opts.RawTopicFilter),
			},
		}
	}
	resp, err := c.CallMethod(req)
	if err != nil {
		return "", enrichSOAPErr(resp, err)
	}
	body, err := readClose(resp)
	if err != nil {
		return "", err
	}
	var decoded event.CreatePullPointSubscriptionResponse
	if err := unmarshalNode(body, "CreatePullPointSubscriptionResponse", &decoded); err != nil {
		return "", err
	}
	addr := string(decoded.SubscriptionReference.Address)
	if addr == "" {
		return "", errors.New("CreatePullPointSubscription response has empty SubscriptionReference Address")
	}
	return addr, nil
}

// pullMessages returns an empty slice (no error) when the camera had
// nothing within PullTimeout.
func pullMessages(c caller, endpoint string, opts Options) ([]event.NotificationMessage, error) {
	req := event.PullMessages{
		Timeout:      xsd.Duration(durationToXSD(opts.PullTimeout)),
		MessageLimit: xsd.Int(opts.MessageLimit),
	}
	body, err := xml.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal PullMessages: %w", err)
	}
	resp, err := c.SendSoap(endpoint, string(body))
	if err != nil {
		return nil, enrichSOAPErr(resp, err)
	}
	respBody, err := readClose(resp)
	if err != nil {
		return nil, err
	}
	var decoded event.PullMessagesResponse
	if err := unmarshalNode(respBody, "PullMessagesResponse", &decoded); err != nil {
		return nil, err
	}
	return decoded.NotificationMessage, nil
}

// unsubscribePullPoint is best-effort. Empty endpoint is a no-op
// (construction failed before installing one).
func unsubscribePullPoint(c caller, endpoint string) error {
	if endpoint == "" {
		return nil
	}
	body, err := xml.Marshal(event.Unsubscribe{})
	if err != nil {
		return fmt.Errorf("marshal Unsubscribe: %w", err)
	}
	resp, err := c.SendSoap(endpoint, string(body))
	if err != nil {
		return enrichSOAPErr(resp, err)
	}
	_, err = readClose(resp)
	return err
}

func readClose(resp *http.Response) (string, error) {
	if resp == nil || resp.Body == nil {
		return "", errors.New("nil HTTP response")
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return "", fmt.Errorf("read response body: %w", err)
	}
	return string(b), nil
}

// unmarshalNode finds the first XML start element with the given local
// name and decodes it into out. ONVIF SOAP responses are wrapped in an
// envelope with many namespace prefixes; keying on local name only
// sidesteps namespace matching.
//
// When the camera returns a SOAP Fault, the fault reason is returned
// as the error so callers can distinguish auth / expired-subscription
// from "unparseable response".
func unmarshalNode(body, localName string, out any) error {
	if reason := extractSOAPFault(body); reason != "" {
		return fmt.Errorf("ONVIF SOAP fault: %s", reason)
	}
	dec := xml.NewDecoder(bytes.NewBufferString(body))
	for {
		tok, err := dec.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("ONVIF response missing %s element", localName)
			}
			return fmt.Errorf("scan ONVIF response: %w", err)
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		if start.Name.Local != localName {
			continue
		}
		if err := dec.DecodeElement(out, &start); err != nil {
			return fmt.Errorf("decode %s: %w", localName, err)
		}
		return nil
	}
}

var (
	// SOAP 1.1: <faultstring>reason</faultstring>
	soap11FaultRE = regexp.MustCompile(`(?s)<(?:[^:>\s]+:)?faultstring[^>]*>(.*?)</(?:[^:>\s]+:)?faultstring>`)
	// SOAP 1.2: <Fault>...<Reason><Text>reason</Text></Reason>...</Fault>
	soap12FaultRE = regexp.MustCompile(`(?s)<(?:[^:>\s]+:)?Reason\b[^>]*>.*?<(?:[^:>\s]+:)?Text[^>]*>(.*?)</(?:[^:>\s]+:)?Text>`)
	// SOAP 1.2 Subcode: <Code>...<Subcode><Value>ter:InvalidArgs</Value></Subcode>...
	soap12SubcodeRE = regexp.MustCompile(`(?s)<(?:[^:>\s]+:)?Subcode\b[^>]*>.*?<(?:[^:>\s]+:)?Value[^>]*>(.*?)</(?:[^:>\s]+:)?Value>`)
)

// extractSOAPFault returns the reason text from a SOAP fault or empty
// when the body is not a fault. Handles SOAP 1.1 (faultstring) and
// SOAP 1.2 (Reason/Text) shapes.
func extractSOAPFault(body string) string {
	if !strings.Contains(body, "Fault") {
		return ""
	}
	if m := soap11FaultRE.FindStringSubmatch(body); len(m) > 1 {
		return strings.TrimSpace(m[1])
	}
	if m := soap12FaultRE.FindStringSubmatch(body); len(m) > 1 {
		return strings.TrimSpace(m[1])
	}
	return ""
}

// extractSOAPSubcode is the fallback when Reason/Text is empty — AXIS
// routinely sends an empty <Text/> alongside a populated Subcode
// (e.g. "ter:InvalidArgs"), and that subcode is the only actionable
// signal the operator gets.
func extractSOAPSubcode(body string) string {
	m := soap12SubcodeRE.FindStringSubmatch(body)
	if len(m) > 1 {
		return strings.TrimSpace(m[1])
	}
	return ""
}

// maxErrExcerpt caps the body excerpt appended to an enriched error so
// a wedged camera streaming a multi-megabyte HTML error page can not
// flood logs with every retry.
const maxErrExcerpt = 512

// enrichSOAPErr appends the camera's actual complaint (SOAP Fault
// reason, then Subcode, then raw body excerpt) to a transport error so
// operators see *why* the camera said 400 instead of just "400 Bad
// Request". The original err is preserved via %w for errors.Is/As.
func enrichSOAPErr(resp *http.Response, err error) error {
	if err == nil {
		return nil
	}
	if resp == nil || resp.Body == nil {
		return err
	}
	defer resp.Body.Close()
	b, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if readErr != nil || len(b) == 0 {
		return err
	}
	body := string(b)
	if reason := extractSOAPFault(body); reason != "" {
		return fmt.Errorf("%w: SOAP fault: %s", err, reason)
	}
	if sub := extractSOAPSubcode(body); sub != "" {
		return fmt.Errorf("%w: SOAP fault subcode: %s", err, sub)
	}
	excerpt := strings.TrimSpace(body)
	if len(excerpt) > maxErrExcerpt {
		excerpt = excerpt[:maxErrExcerpt] + "...(truncated)"
	}
	return fmt.Errorf("%w: response body: %s", err, excerpt)
}

// durationToXSD formats a duration as xsd:duration PTnS. Second
// precision is sufficient — ONVIF cameras do not honour sub-second
// pull timeouts.
func durationToXSD(d time.Duration) string {
	secs := int(d.Round(time.Second).Seconds())
	if secs <= 0 {
		secs = 1
	}
	return "PT" + strconv.Itoa(secs) + "S"
}
