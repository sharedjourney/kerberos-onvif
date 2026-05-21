// Package stream is a typed, channel-based consumer for ONVIF device
// events. It hides the SOAP/XML, pull-point subscription lifecycle,
// subscription renewal and vendor-specific topic conventions behind a
// single Event stream.
//
// # Usage
//
//	dev, _ := onvif.NewDevice(onvif.DeviceParams{Xaddr: "...", Username: "...", Password: "..."})
//	s, err := stream.NewStream(ctx, dev, stream.Options{DeviceID: "front-door"})
//	if err != nil { /* construction failed: auth, network, or camera does not advertise events */ }
//	defer s.Close()
//
//	for ev := range s.Events() {
//	    switch ev.Kind {
//	    case stream.KindMotion:
//	        if ev.State == stream.StateActive { /* start recording */ }
//	    }
//	}
//
// # Invariants
//
// NewStream performs network I/O. It returns once the
// CreatePullPointSubscription call has succeeded; auth and reachability
// failures surface as an error from NewStream rather than landing on
// the Errors channel later.
//
// Two goroutines back each Stream: a pull loop and a renew loop. Both
// exit when the context passed to NewStream is cancelled or when Close
// is called. Close is idempotent and bounded — see Stream.Close.
//
// Events is closed exactly when the Stream stops. Ranging over Events
// is safe; a closed channel terminates the loop without a Close call.
// Errors is also closed at stop time. Both channels are buffered (16
// slots by default); sends to Errors are non-blocking so a stalled
// consumer drops older errors rather than the pull loop blocking on
// log output.
//
// The decoded Event preserves the wire form (Topic, raw Source and
// Data maps) so callers can fall back to inspecting non-standard
// payloads when Kind is KindUnknown.
//
// # Reconnect
//
// On ReconnectAfterFailures consecutive PullMessages failures the
// Stream silently recreates its pull-point subscription. ONVIF cameras
// replay each property's current value with PropertyInitialized on a
// new subscription; Events delivered between recreate and the first
// non-Initialized event carry Event.AfterReconnect=true so consumers
// can suppress duplicate handling.
//
// Set Options.DisableReconnect=true to opt out of recreate; the pull
// loop will retry against the original subscription until ctx cancel.
//
// # Topic classification
//
// Classify maps ONVIF topic strings to a small set of normalized Kind
// values across AXIS, Hikvision, Avigilon, Hanwha, Bosch and Dahua. See
// topics.go for the verified mapping table with public-doc citations.
package stream
