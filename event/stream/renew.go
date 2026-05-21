package stream

import (
	"context"
	"encoding/xml"
	"fmt"
	"time"

	"github.com/kerberos-io/onvif/event"
	"github.com/kerberos-io/onvif/xsd"
)

// renewLoop refreshes the subscription before InitialTermination expires.
// Exits when ctx is cancelled. Renew failures surface as ErrRenewFailed
// on the Errors channel; the loop continues because a permanently
// failing renew will eventually drop the subscription and the pull
// loop's reconnect path will recover (recreate is the only reliable
// recovery once a subscription is GC'd at the camera).
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

// renewPullPoint issues a wsnt:Renew SOAP against the given
// subscription endpoint with an absolute TerminationTime.
//
// WS-BaseNotification §6.1.1 declares TerminationTime as xsd:dateTime
// OR xsd:duration, but older Hikvision, some Dahua and some Bosch
// firmwares reject the relative-duration form. We send an absolute
// UTC datetime to match what production NVRs do.
func renewPullPoint(c caller, endpoint string, opts Options) error {
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
