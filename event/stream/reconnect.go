package stream

import (
	"context"
	"math/rand"
	"time"
)

// maxRecreateBackoff caps exponential backoff between recreate attempts.
// Sized for fleet deployments: a 1000-camera setup recovering from a
// switch reboot would otherwise hammer the network with one recreate
// attempt per camera per 30s; 5 minutes gives the network time to
// settle while still recovering promptly when a single camera comes
// back.
const maxRecreateBackoff = 5 * time.Minute

// jitterFraction is the symmetric jitter applied to recreate backoff:
// the actual sleep is sampled from [backoff*(1-jitter), backoff*(1+jitter)].
// Prevents thundering-herd reconnects when many cameras drop together
// (switch reboot, NAT timeout).
const jitterFraction = 0.25

// pullLoop is the main pull goroutine of a Stream. It calls
// PullMessages in a tight loop, decodes results into Events and feeds
// the Events channel.
//
// After ReconnectAfterFailures consecutive pull errors it asks
// attemptRecreate to recreate the pull-point subscription, marking the
// next batch's events with AfterReconnect so consumers can suppress
// duplicate handling of the ONVIF Initialized-replay that follows a
// new subscription.
//
// Exits when ctx is cancelled.
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
		if !sleepCtx(ctx, jitter(*backoff)) {
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

// jitter returns d perturbed by ±jitterFraction. Used to spread
// recreate attempts across a fleet so a synchronised drop (switch
// reboot, DHCP storm) does not cause a synchronised reconnect surge.
// Returns at least 1ns to keep sleepCtx happy.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return time.Nanosecond
	}
	spread := float64(d) * jitterFraction
	delta := (rand.Float64()*2 - 1) * spread
	out := time.Duration(float64(d) + delta)
	if out <= 0 {
		out = time.Nanosecond
	}
	return out
}
