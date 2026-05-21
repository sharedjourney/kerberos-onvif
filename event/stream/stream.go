package stream

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/kerberos-io/onvif"
	"github.com/kerberos-io/onvif/event"
	"github.com/kerberos-io/onvif/xsd"
)

// Options configures a Stream. The zero value is usable; defaultOptions
// fills in production-sensible defaults for any unset field.
type Options struct {
	// DeviceID identifies the camera in emitted Events. Recommended so a
	// single channel can fan in multiple cameras. Empty is allowed.
	DeviceID string
	// TopicFilter is the raw ONVIF ConcreteSet TopicExpression filter
	// passed to CreatePullPointSubscription. The empty string means no
	// filter — required for AXIS, accepted by every other vendor we
	// support. Callers should normally leave this empty and rely on
	// Classify for routing.
	TopicFilter string
	// PullTimeout is the server-side wait time in each PullMessages call
	// (xsd:duration). The camera returns early when messages are
	// available; otherwise it returns empty after this timeout. Default:
	// 5s.
	PullTimeout time.Duration
	// MessageLimit caps the number of NotificationMessage entries
	// returned per PullMessages call. Default: 10.
	MessageLimit int
	// InitialTermination is the requested subscription lifetime passed
	// to CreatePullPointSubscription. The renew loop (added in a later
	// commit) will refresh well before this expires. Default: 60s.
	InitialTermination time.Duration
	// BufferSize is the buffer size of the Events and Errors channels.
	// Larger buffers absorb consumer hiccups at the cost of memory.
	// Default: 16.
	BufferSize int
}

func defaultOptions() Options {
	return Options{
		PullTimeout:        5 * time.Second,
		MessageLimit:       10,
		InitialTermination: 60 * time.Second,
		BufferSize:         16,
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
	if o.BufferSize > 0 {
		d.BufferSize = o.BufferSize
	}
	d.DeviceID = o.DeviceID
	d.TopicFilter = o.TopicFilter
	return d
}

// caller is the subset of *onvif.Device the Stream depends on. Tests
// substitute a fake; production code uses the device adapter.
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
	caller    caller
	opts      Options
	pullPoint string

	events chan Event
	errors chan error

	cancel context.CancelFunc
	done   chan struct{}

	closeOnce sync.Once
	closeErr  error

	// now is overridable in tests to make timestamps deterministic.
	now func() time.Time
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
func (s *Stream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		<-s.done
		// Unsubscribe is best-effort: if the camera is unreachable
		// the subscription will expire on its own at
		// InitialTermination + Renew interval anyway.
		if err := unsubscribePullPoint(s.caller, s.pullPoint); err != nil {
			s.closeErr = fmt.Errorf("unsubscribe pull point: %w", err)
		}
	})
	return s.closeErr
}

func (s *Stream) run(ctx context.Context) {
	defer close(s.done)
	defer close(s.events)
	defer close(s.errors)

	for {
		if ctx.Err() != nil {
			return
		}
		msgs, err := pullMessages(s.caller, s.pullPoint, s.opts)
		if err != nil {
			s.surfaceError(err)
			// Brief backoff before retrying; reconnect-on-error
			// lands in a follow-up commit and replaces this with
			// proper subscription recreation.
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}
		observedAt := s.now()
		for _, m := range msgs {
			ev := Decode(m, s.opts.DeviceID, observedAt)
			select {
			case <-ctx.Done():
				return
			case s.events <- ev:
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
	if opts.TopicFilter != "" {
		req.Filter = &event.FilterType{
			TopicExpression: &event.TopicExpressionType{
				Dialect:    xsd.String("http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet"),
				TopicKinds: xsd.String(opts.TopicFilter),
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
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response body: %w", err)
	}
	return string(b), nil
}

// unmarshalNode finds the first XML start element with the given local
// name and decodes it into out. ONVIF SOAP responses come wrapped in an
// envelope with multiple namespace prefixes; this helper sidesteps
// namespace matching by keying on local name only.
func unmarshalNode(body, localName string, out any) error {
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
