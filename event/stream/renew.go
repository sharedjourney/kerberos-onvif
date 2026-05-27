package stream

import (
	"context"
	"encoding/xml"
	"fmt"
	"time"

	"github.com/kerberos-io/onvif/event"
	"github.com/kerberos-io/onvif/xsd"
)

// renewLoop surfaces renew failures and continues. A permanently
// failing renew lets the subscription die at the camera; the pull
// loop's reconnect path then recreates it — recreate is the only
// reliable recovery once a subscription is GC'd.
func (s *Stream) renewLoop(ctx context.Context) {
	interval := s.opts.InitialTermination - s.opts.RenewMargin
	if interval <= 0 {
		// Pathological config (margin >= termination): renew at
		// half termination so we still refresh.
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

// renewPullPoint sends Renew with an absolute UTC TerminationTime.
// WS-BaseNotification §6.1.1 also allows xsd:duration but older
// Hikvision, some Dahua and some Bosch firmwares reject the
// relative form.
func renewPullPoint(c caller, ref subscriptionRef, opts Options) error {
	absoluteEnd := time.Now().UTC().Add(opts.InitialTermination).Format("2006-01-02T15:04:05Z")
	req := event.Renew{TerminationTime: xsd.String(absoluteEnd)}
	body, err := xml.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal Renew: %w", err)
	}
	headerXML, err := buildRefParamsHeader(ref.RefParamsXML)
	if err != nil {
		return fmt.Errorf("build ref params header: %w", err)
	}
	resp, err := c.SendSoapWithHeader(ref.Address, string(body), headerXML)
	if err != nil {
		return enrichSOAPErr(resp, err)
	}
	_, err = readClose(resp)
	return err
}
