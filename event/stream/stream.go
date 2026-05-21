package stream

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kerberos-io/onvif"
	"github.com/kerberos-io/onvif/event"
	"github.com/kerberos-io/onvif/xsd"
)

// maxResponseBytes caps the size of a SOAP response we will buffer in
// memory. ONVIF PullMessages bodies are normally <100KB even with dense
// analytics payloads; 10 MiB is comfortably above legitimate traffic
// while keeping a hostile or buggy camera from OOMing the process.
const maxResponseBytes = 10 << 20

// closeUnsubscribeTimeout bounds the SOAP Unsubscribe call issued by
// Close so a hung camera connection cannot wedge the caller. The
// subscription expires at the camera anyway once InitialTermination
// elapses, so a missed unsubscribe is at worst cosmetic.
const closeUnsubscribeTimeout = 5 * time.Second

// Options configures a Stream.
//
// Zero-value policy: every duration / int field treats zero as "use the
// default". To opt out of reconnect entirely set DisableReconnect=true
// (sentinel `ReconnectAfterFailures=0` would otherwise collide with the
// default-injection policy). To get a synchronous (unbuffered) channel
// pair set BufferSize=-1.
type Options struct {
	// DeviceID identifies the camera in emitted Events. Recommended so
	// a single channel can fan in multiple cameras. Empty is allowed.
	DeviceID string
	// RawTopicFilter is the raw ONVIF ConcreteSet TopicExpression
	// filter passed to CreatePullPointSubscription. Empty means no
	// filter — required for AXIS, accepted by every other vendor we
	// support. The name carries 'Raw' because the value is fed verbatim
	// into the SOAP envelope: callers should normally leave it empty
	// and rely on Classify for routing rather than ask the camera to
	// filter server-side, which is fragile across vendors.
	RawTopicFilter string
	// PullTimeout is the server-side wait time in each PullMessages
	// call (xsd:duration). The camera returns early when messages are
	// available; otherwise it returns empty after this timeout. Zero
	// means default (5s).
	PullTimeout time.Duration
	// MessageLimit caps the number of NotificationMessage entries
	// returned per PullMessages call. Zero means default (32). A busy
	// AXIS with many configured inputs can burst beyond 10 per pull;
	// 32 covers that without significantly enlarging quiet pulls.
	MessageLimit int
	// InitialTermination is the requested subscription lifetime passed
	// to CreatePullPointSubscription. The renew loop refreshes well
	// before this expires. Zero means default (60s).
	InitialTermination time.Duration
	// RenewMargin is how long before InitialTermination expiry the
	// renew loop fires. Larger margins tolerate slower networks at the
	// cost of more renew SOAP calls. Zero means default (10s).
	RenewMargin time.Duration
	// ReconnectAfterFailures is the consecutive PullMessages failure
	// count that triggers a CreatePullPointSubscription recreate. The
	// camera or pull-point can die for many reasons (camera reboot,
	// subscription garbage-collected after a renew miss, intermediate
	// NAT timeout); rebuilding the subscription is the only reliable
	// recovery. Zero means default (3). To disable reconnect entirely
	// set DisableReconnect=true.
	ReconnectAfterFailures int
	// DisableReconnect skips automatic CreatePullPointSubscription
	// recreate. The pull loop will continue retrying against the
	// original endpoint until ctx is cancelled. Useful for tests or
	// callers managing recovery externally.
	DisableReconnect bool
	// RetryBackoff is the initial sleep between a pull/recreate failure
	// and the next attempt. Recreate failures double this up to a 30s
	// ceiling. Zero means default (1s).
	RetryBackoff time.Duration
	// BufferSize is the buffer size of the Events and Errors channels.
	// Larger buffers absorb consumer hiccups at the cost of memory.
	// Zero means default (16); use -1 for unbuffered (synchronous)
	// channels.
	BufferSize int
}

func defaultOptions() Options {
	return Options{
		PullTimeout:            5 * time.Second,
		MessageLimit:           32,
		InitialTermination:     60 * time.Second,
		RenewMargin:            10 * time.Second,
		ReconnectAfterFailures: 3,
		RetryBackoff:           time.Second,
		BufferSize:             16,
	}
}

func (o Options) withDefaults() Options {
	d := defaultOptions()
	if o.PullTimeout > 0 {
		d.PullTimeout = o.PullTimeout
	}
	if o.MessageLimit > 0 {
		d.MessageLimit = o.MessageLimit
	}
	if o.InitialTermination > 0 {
		d.InitialTermination = o.InitialTermination
	}
	if o.RenewMargin > 0 {
		d.RenewMargin = o.RenewMargin
	}
	if o.ReconnectAfterFailures > 0 {
		d.ReconnectAfterFailures = o.ReconnectAfterFailures
	}
	if o.RetryBackoff > 0 {
		d.RetryBackoff = o.RetryBackoff
	}
	// BufferSize: zero -> default; negative -> 0 (unbuffered).
	switch {
	case o.BufferSize > 0:
		d.BufferSize = o.BufferSize
	case o.BufferSize < 0:
		d.BufferSize = 0
	}
	d.DeviceID = o.DeviceID
	d.RawTopicFilter = o.RawTopicFilter
	d.DisableReconnect = o.DisableReconnect
	return d
}

// maxRecreateBackoff caps exponential backoff between recreate attempts.
const maxRecreateBackoff = 30 * time.Second

// caller is the subset of *onvif.Device the Stream depends on. Tests
// substitute a fake; production code uses the device adapter.
//
// Implementations must be safe for concurrent use: the pull loop and
// renew loop call into caller from separate goroutines. *onvif.Device
// satisfies this because its HTTP client is the goroutine-safe
// http.Client.
type caller interface {
	CallMethod(method any) (*http.Response, error)
	SendSoap(endpoint, body string) (*http.Response, error)
}

type deviceCaller struct{ dev *onvif.Device }

func (d deviceCaller) CallMethod(m any) (*http.Response, error) {
	return d.dev.CallMethod(m)
}

func (d deviceCaller) SendSoap(endpoint, body string) (*http.Response, error) {
	return d.dev.SendSoap(endpoint, body)
}

// Stream owns a single ONVIF pull-point subscription and surfaces the
// decoded notifications on a typed channel. Close stops the background
// goroutine and unsubscribes from the camera.
//
// A Stream is safe for concurrent use by Close from any goroutine while
// readers consume Events / Errors; Close is idempotent.
type Stream struct {
	caller caller
	opts   Options

	pullPointMu sync.Mutex
	pullPoint   string

	events chan Event
	errors chan error

	cancel context.CancelFunc
	done   chan struct{}

	closeOnce sync.Once
	closeErr  error

	// now is overridable in tests to make timestamps deterministic.
	now func() time.Time
}

func (s *Stream) getPullPoint() string {
	s.pullPointMu.Lock()
	defer s.pullPointMu.Unlock()
	return s.pullPoint
}

func (s *Stream) setPullPoint(addr string) {
	s.pullPointMu.Lock()
	defer s.pullPointMu.Unlock()
	s.pullPoint = addr
}

// NewStream creates a Stream against an ONVIF device. It performs the
// CreatePullPointSubscription call synchronously so connectivity and
// authentication problems surface immediately as an error rather than
// landing on the Errors channel later. The background pull loop starts
// before NewStream returns.
//
// The returned Stream stops when ctx is cancelled or when Close is
// called.
func NewStream(ctx context.Context, dev *onvif.Device, opts Options) (*Stream, error) {
	return newStream(ctx, deviceCaller{dev: dev}, opts)
}

func newStream(ctx context.Context, c caller, opts Options) (*Stream, error) {
	opts = opts.withDefaults()
	addr, err := createPullPoint(c, opts)
	if err != nil {
		return nil, fmt.Errorf("create pull point subscription: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	s := &Stream{
		caller:    c,
		opts:      opts,
		pullPoint: addr,
		events:    make(chan Event, opts.BufferSize),
		errors:    make(chan error, opts.BufferSize),
		cancel:    cancel,
		done:      make(chan struct{}),
		now:       time.Now,
	}
	go s.run(runCtx)
	return s, nil
}

// Events returns the channel of decoded notifications. The channel is
// closed when the Stream stops.
func (s *Stream) Events() <-chan Event { return s.events }

// Errors returns the channel of non-fatal errors encountered while
// pulling. Sends are non-blocking, so consumers that fall behind drop
// older errors. The channel is closed when the Stream stops.
func (s *Stream) Errors() <-chan error { return s.errors }

// Close stops the background goroutine, waits for it to exit, and
// unsubscribes from the camera. Subsequent calls are no-ops.
//
// Unsubscribe is bounded by closeUnsubscribeTimeout so a hung camera
// connection cannot wedge the caller. On timeout Close still returns
// promptly; the subscription will expire at the camera once
// InitialTermination + RenewMargin elapses without a renew.
func (s *Stream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		<-s.done

		errCh := make(chan error, 1)
		go func() {
			errCh <- unsubscribePullPoint(s.caller, s.getPullPoint())
		}()
		select {
		case err := <-errCh:
			if err != nil {
				s.closeErr = fmt.Errorf("unsubscribe pull point: %w", err)
			}
		case <-time.After(closeUnsubscribeTimeout):
			s.closeErr = fmt.Errorf("unsubscribe pull point: timeout after %s", closeUnsubscribeTimeout)
		}
	})
	return s.closeErr
}

func (s *Stream) run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.renewLoop(ctx)
	}()
	s.pullLoop(ctx)
	wg.Wait()

	// Explicit close order after both goroutines have exited so a
	// future maintainer extending this function does not accidentally
	// rely on defer-ordering for channel-close safety.
	close(s.errors)
	close(s.events)
	close(s.done)
}

func (s *Stream) pullLoop(ctx context.Context) {
	var failures int
	recreateBackoff := s.opts.RetryBackoff
	var afterReconnect bool

	for {
		if ctx.Err() != nil {
			return
		}
		msgs, err := pullMessages(s.caller, s.getPullPoint(), s.opts)
		if err != nil {
			s.surfaceError(ErrPullFailed{Err: err})
			failures++
			if !s.opts.DisableReconnect && failures >= s.opts.ReconnectAfterFailures {
				justRecreated, cont := s.attemptRecreate(ctx, &failures, &recreateBackoff)
				if !cont {
					return
				}
				if justRecreated {
					afterReconnect = true
				}
				continue
			}
			if !sleepCtx(ctx, s.opts.RetryBackoff) {
				return
			}
			continue
		}
		// Successful pull resets failure tracking.
		failures = 0
		recreateBackoff = s.opts.RetryBackoff
		observedAt := s.now()
		for _, m := range msgs {
			ev := decode(m, s.opts.DeviceID, observedAt)
			if afterReconnect {
				ev.AfterReconnect = true
				// ONVIF replays current state with
				// PropertyInitialized on a new subscription.
				// Clear the flag as soon as we see anything
				// other than Initialized — at that point we
				// have transitioned to live events.
				if ev.Operation != PropertyInitialized {
					afterReconnect = false
				}
			}
			select {
			case <-ctx.Done():
				return
			case s.events <- ev:
			}
		}
	}
}

// attemptRecreate calls CreatePullPointSubscription and on success
// installs the new endpoint atomically. The first return is true when
// recreate succeeded just now (caller flags the next batch with
// AfterReconnect). The second return is false only if ctx was cancelled
// during backoff (caller should exit the run loop).
func (s *Stream) attemptRecreate(ctx context.Context, failures *int, backoff *time.Duration) (justRecreated, cont bool) {
	addr, err := createPullPoint(s.caller, s.opts)
	if err != nil {
		s.surfaceError(ErrRecreateFailed{Err: err})
		if !sleepCtx(ctx, *backoff) {
			return false, false
		}
		*backoff *= 2
		if *backoff > maxRecreateBackoff {
			*backoff = maxRecreateBackoff
		}
		return false, true
	}
	s.setPullPoint(addr)
	*failures = 0
	*backoff = s.opts.RetryBackoff
	return true, true
}

// renewLoop refreshes the subscription before InitialTermination expires.
// Exits when ctx is cancelled.
func (s *Stream) renewLoop(ctx context.Context) {
	interval := s.opts.InitialTermination - s.opts.RenewMargin
	if interval <= 0 {
		// Pathological config (margin >= termination): fall back to
		// renewing at half the termination so we still refresh,
		// rather than busy-looping or never renewing.
		interval = s.opts.InitialTermination / 2
		if interval <= 0 {
			interval = time.Second
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := renewPullPoint(s.caller, s.getPullPoint(), s.opts); err != nil {
				s.surfaceError(ErrRenewFailed{Err: err})
			}
		}
	}
}

// surfaceError sends err on the errors channel non-blockingly so a
// stalled consumer cannot block the pull loop.
func (s *Stream) surfaceError(err error) {
	select {
	case s.errors <- err:
	default:
	}
}

// sleepCtx blocks for d or until ctx is cancelled. Returns true if d
// elapsed, false if ctx was cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// --- SOAP helpers (unexported) ----------------------------------------

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
		return "", err
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
		return nil, err
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

func renewPullPoint(c caller, endpoint string, opts Options) error {
	// WS-BaseNotification §6.1.1 declares TerminationTime as
	// xsd:dateTime OR xsd:duration, but older Hikvision, some Dahua
	// and some Bosch firmwares reject the relative-duration form. Send
	// an absolute UTC datetime to match what production NVRs do.
	absoluteEnd := time.Now().UTC().Add(opts.InitialTermination).Format("2006-01-02T15:04:05Z")
	req := event.Renew{TerminationTime: xsd.String(absoluteEnd)}
	body, err := xml.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal Renew: %w", err)
	}
	resp, err := c.SendSoap(endpoint, string(body))
	if err != nil {
		return err
	}
	_, err = readClose(resp)
	return err
}

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
		return err
	}
	_, err = readClose(resp)
	return err
}

func readClose(resp *http.Response) (string, error) {
	if resp == nil || resp.Body == nil {
		return "", errors.New("nil HTTP response")
	}
	defer resp.Body.Close()
	// LimitReader prevents a hostile or buggy camera from OOMing the
	// agent by streaming an unbounded response body.
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return "", fmt.Errorf("read response body: %w", err)
	}
	return string(b), nil
}

// unmarshalNode finds the first XML start element with the given local
// name and decodes it into out. ONVIF SOAP responses come wrapped in an
// envelope with multiple namespace prefixes; this helper sidesteps
// namespace matching by keying on local name only.
//
// When the camera returns a SOAP Fault instead of the expected
// response, the fault reason is surfaced as the error so callers can
// distinguish "auth failed" / "subscription expired" from "unparseable
// response".
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
)

// extractSOAPFault returns the human-readable reason text from a SOAP
// fault, or empty string when the body is not a fault. Handles both
// SOAP 1.1 (faultstring) and SOAP 1.2 (Reason/Text) shapes.
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

// durationToXSD formats a Go time.Duration as an xsd:duration string in
// PTnS form. Second precision is sufficient — ONVIF cameras do not
// honour sub-second pull timeouts and intermediate routers may round in
// any case.
func durationToXSD(d time.Duration) string {
	secs := int(d.Round(time.Second).Seconds())
	if secs <= 0 {
		secs = 1
	}
	return "PT" + strconv.Itoa(secs) + "S"
}
