package stream

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/kerberos-io/onvif"
)

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

// run orchestrates the pull and renew goroutines and closes the
// emission channels once both have exited.
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

// surfaceError sends err on the errors channel non-blockingly so a
// stalled consumer cannot block the pull or renew loop.
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
