package stream

import (
	"fmt"
	"time"
)

// EventKind is the normalized category of an ONVIF event, independent of the
// camera vendor's topic naming.
type EventKind uint8

const (
	// KindUnknown is the zero value; used when a topic does not match any
	// known classification.
	KindUnknown EventKind = iota
	// KindMotion covers motion detection from any vendor (e.g. AXIS
	// VideoSource/MotionAlarm, Hikvision RuleEngine/CellMotionDetector).
	KindMotion
	// KindTampering covers camera tampering / scene change alarms.
	KindTampering
	// KindDigitalInput covers external sensor inputs wired to the camera.
	KindDigitalInput
	// KindDigitalOutput covers relay output state changes on the camera.
	KindDigitalOutput
	// KindObjectDetected covers analytics-based object/person/vehicle
	// detection events.
	KindObjectDetected
	// KindAudioAlarm covers audio-level / loud-noise alarms.
	KindAudioAlarm
)

// String implements fmt.Stringer.
func (k EventKind) String() string {
	switch k {
	case KindUnknown:
		return "Unknown"
	case KindMotion:
		return "Motion"
	case KindTampering:
		return "Tampering"
	case KindDigitalInput:
		return "DigitalInput"
	case KindDigitalOutput:
		return "DigitalOutput"
	case KindObjectDetected:
		return "ObjectDetected"
	case KindAudioAlarm:
		return "AudioAlarm"
	default:
		return fmt.Sprintf("EventKind(%d)", uint8(k))
	}
}

// EventState is the active/inactive state carried by an event. Most ONVIF
// alarms are boolean (e.g. IsMotion=true/false); StateUnknown is used when
// the value cannot be parsed.
type EventState uint8

const (
	StateUnknown EventState = iota
	StateActive
	StateInactive
)

// String implements fmt.Stringer.
func (s EventState) String() string {
	switch s {
	case StateUnknown:
		return "Unknown"
	case StateActive:
		return "Active"
	case StateInactive:
		return "Inactive"
	default:
		return fmt.Sprintf("EventState(%d)", uint8(s))
	}
}

// PropertyOperation mirrors the ONVIF wsnt:PropertyOperation attribute and
// indicates whether a message is the first sighting of a property
// (Initialized), a transition (Changed) or the property going away (Deleted).
type PropertyOperation uint8

const (
	PropertyUnknown PropertyOperation = iota
	PropertyInitialized
	PropertyChanged
	PropertyDeleted
)

// String implements fmt.Stringer.
func (p PropertyOperation) String() string {
	switch p {
	case PropertyUnknown:
		return "Unknown"
	case PropertyInitialized:
		return "Initialized"
	case PropertyChanged:
		return "Changed"
	case PropertyDeleted:
		return "Deleted"
	default:
		return fmt.Sprintf("PropertyOperation(%d)", uint8(p))
	}
}

// Event is a single normalized notification from an ONVIF device.
//
// Kind, State and Operation are the normalized fields most callers should
// switch on. Topic, RawValue and Source preserve the original ONVIF data so
// callers can do further inspection or logging without re-parsing SOAP.
type Event struct {
	// Kind is the normalized event category.
	Kind EventKind
	// State is the active/inactive value carried by the event.
	State EventState
	// Operation is the ONVIF property lifecycle (Initialized/Changed/Deleted).
	Operation PropertyOperation
	// Source identifies the channel, input, or rule that produced the event
	// (taken from the Source SimpleItem in the notification).
	Source string
	// Topic is the raw ONVIF topic string, e.g. tns1:VideoSource/MotionAlarm.
	Topic string
	// RawValue is the unparsed Data SimpleItem value (e.g. "true", "1",
	// "active") so callers can read non-boolean values when needed.
	RawValue string
	// Timestamp is when the stream observed the event locally. The ONVIF
	// UtcTime is not used because clocks on many cameras drift.
	Timestamp time.Time
}
