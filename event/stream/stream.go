package stream

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/kerberos-io/onvif"
)

// closeUnsubscribeTimeout bounds the Unsubscribe SOAP call issued by
// Close. A subscription expires at the camera once InitialTermination
// elapses without a renew, so a missed unsubscribe is at worst
// cosmetic.
const closeUnsubscribeTimeout = 5 * time.Second

// Options configures a Stream.
//
// Zero-value policy: every duration / int field treats zero as "use
// the default". To opt out of reconnect set DisableReconnect=true
// (ReconnectAfterFailures=0 would otherwise collide with the default
// injection). For unbuffered Events / Errors channels set
// BufferSize=-1.
type Options struct {
	DeviceID string
	// RawTopicFilter is the ONVIF ConcreteSet TopicExpression filter
	// passed verbatim to CreatePullPointSubscription. Callers should
	// normally leave this empty and rely on Classify for routing —
	// server-side filtering is fragile across vendors and empty is
	// required for AXIS.
	RawTopicFilter string
	// PullTimeout — zero means default (5s).
	PullTimeout time.Duration
	// MessageLimit — zero means default (32). Busy AXIS cameras with
	// many configured rules can burst beyond 10 per pull.
	MessageLimit int
	// InitialTermination — zero means default (60s).
	InitialTermination time.Duration
	// RenewMargin — larger margins tolerate slower networks at the
	// cost of more renew calls. Zero means default (10s).
	RenewMargin time.Duration
	// ReconnectAfterFailures — pull-points die for many reasons
	// (camera reboot, subscription GC after a renew miss, NAT
	// timeout); rebuilding the subscription is the only reliable
	// recovery. Zero means default (3). Set DisableReconnect=true
	// to disable.
	ReconnectAfterFailures int
	// DisableReconnect makes the pull loop retry against the
	// original endpoint until ctx is cancelled.
	DisableReconnect bool
	// RetryBackoff is the base sleep between pull/recreate failures.
	// Recreate failures double this up to maxRecreateBackoff. Zero
	// means default (1s).
	RetryBackoff time.Duration
	// BufferSize — zero means default (16); use -1 for unbuffered.
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

// caller is the *onvif.Device subset Stream depends on. Implementations
// must be safe for concurrent use — pull and renew goroutines call in
// from separate goroutines. *onvif.Device satisfies this via http.Client.
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

// Stream owns a single ONVIF pull-point subscription. Safe for Close
// from any goroutine while readers consume Events / Errors. Close is
// idempotent.
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

	// now is overridable so tests can make timestamps deterministic.
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

// NewStream creates a Stream and performs CreatePullPointSubscription
// synchronously so connectivity and authentication failures surface
// from NewStream rather than landing on Errors later.
//
// The returned Stream stops when ctx is cancelled or Close is called.
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

// Events returns the channel of decoded notifications. Closed when
// the Stream stops.
func (s *Stream) Events() <-chan Event { return s.events }

// Errors returns the channel of non-fatal errors. Sends are
// non-blocking; consumers that fall behind drop older errors. Closed
// when the Stream stops.
func (s *Stream) Errors() <-chan error { return s.errors }

// Close stops the background goroutines, waits for them to exit and
// Unsubscribes from the camera. Subsequent calls are no-ops.
//
// Unsubscribe is bounded by closeUnsubscribeTimeout so a hung camera
// connection cannot wedge the caller.
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
	// future maintainer extending this function does not rely on
	// defer-ordering for channel-close safety.
	close(s.errors)
	close(s.events)
	close(s.done)
}

func (s *Stream) surfaceError(err error) {
	select {
	case s.errors <- err:
	default:
	}
}

// sleepCtx returns false if ctx was cancelled, true if d elapsed.
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
