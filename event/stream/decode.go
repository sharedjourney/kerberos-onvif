package stream

import (
	"strings"
	"time"

	"github.com/kerberos-io/onvif/event"
)

// Decode converts a single ONVIF NotificationMessage into the package's
// normalized Event representation.
//
// deviceID is supplied by the caller because the message itself does not
// identify the originating camera. observedAt is recorded verbatim as
// Event.Timestamp; the camera-reported wsnt:UtcTime attribute (when
// present and parseable) populates Event.DeviceTime.
//
// When the Topic does not match any classifier rule the returned Event
// has Kind == KindUnknown but Source, Data and Topic are still populated
// so consumers can fall back to inspecting the wire form.
func Decode(msg event.NotificationMessage, deviceID string, observedAt time.Time) Event {
	topic := string(msg.Topic.TopicKinds)
	desc := msg.Message.Message
	return Event{
		Kind:       Classify(topic),
		State:      extractState(desc.Data.SimpleItem),
		Operation:  parsePropertyOperation(string(desc.PropertyOperation)),
		DeviceID:   deviceID,
		Source:     simpleItemsToMap(desc.Source.SimpleItem),
		Data:       simpleItemsToMap(desc.Data.SimpleItem),
		Topic:      topic,
		Timestamp:  observedAt,
		DeviceTime: parseDeviceTime(string(desc.UtcTime)),
	}
}

// simpleItemsToMap collapses ONVIF SimpleItem lists to a Name->Value map.
// Returns nil for an empty list so empty notifications do not allocate
// and match the Event zero-value contract.
func simpleItemsToMap(items []event.SimpleItem) map[string]string {
	if len(items) == 0 {
		return nil
	}
	m := make(map[string]string, len(items))
	for _, it := range items {
		m[string(it.Name)] = string(it.Value)
	}
	return m
}

// extractState scans Data items for a boolean-like value and returns the
// first one as a State. Returns StateUnknown when no item parses — this
// is the correct outcome for edge-triggered topics such as
// LineDetector/Crossed whose Data carries only an ObjectId.
//
// Iteration order over the original []SimpleItem is preserved so the
// behaviour stays deterministic per notification. (Map iteration is not
// involved; simpleItemsToMap is a separate path.)
func extractState(items []event.SimpleItem) State {
	for _, it := range items {
		switch strings.ToLower(strings.TrimSpace(string(it.Value))) {
		case "true", "1", "active":
			return StateActive
		case "false", "0", "inactive":
			return StateInactive
		}
	}
	return StateUnknown
}

// parsePropertyOperation parses the wsnt:PropertyOperation attribute.
// The attribute is optional per WS-Notification; an empty or unrecognised
// value yields PropertyUnknown.
func parsePropertyOperation(s string) PropertyOperation {
	switch s {
	case "Initialized":
		return PropertyInitialized
	case "Changed":
		return PropertyChanged
	case "Deleted":
		return PropertyDeleted
	default:
		return PropertyUnknown
	}
}

// parseDeviceTime parses the wsnt:UtcTime attribute, returning the zero
// time when the attribute is absent or unparseable. The result is
// normalised to UTC so equality comparisons across timezones work.
//
// xsd:dateTime in ONVIF messages is RFC 3339 in practice; we try
// time.RFC3339Nano first (covers sub-second precision) and fall back to
// time.RFC3339 for cameras that drop the fractional part.
func parseDeviceTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
