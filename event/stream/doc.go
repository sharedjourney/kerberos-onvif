// Package stream provides a long-running, channel-based consumer for ONVIF
// device events. It hides the SOAP/XML, pull-point lifecycle, renewal and
// vendor-specific topic conventions behind a typed Event stream.
//
// A Stream is created with NewStream and yields decoded Event values on the
// channel returned by Events. Non-fatal errors (transient SOAP failures that
// the stream recovers from) are surfaced on Errors. The Stream is stopped by
// cancelling the context passed to NewStream or by calling Close.
//
// The package classifies vendor-specific topic strings (AXIS, Hikvision,
// Avigilon, Hanwha, Bosch, Dahua) into a small set of normalized EventKind
// values so callers do not need to special-case device manufacturers.
package stream
