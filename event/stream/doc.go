// Package stream will provide a long-running, channel-based consumer for
// ONVIF device events. It is meant to hide the SOAP/XML, pull-point
// subscription lifecycle, subscription renewal and vendor-specific topic
// conventions behind a typed Event stream.
//
// This file lays down the value types (Kind, State, PropertyOperation,
// Event) and the topic Classifier. The Stream type, its NewStream
// constructor and the Events/Errors channels land in follow-up changes.
//
// The package classifies vendor-specific topic strings (AXIS, Hikvision,
// Avigilon, Hanwha, Bosch, Dahua) into a small set of normalized Kind
// values so callers do not need to special-case device manufacturers.
package stream
