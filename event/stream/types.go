package stream

import (
	"fmt"
	"time"
)

// Kind is the normalized category of an ONVIF event, independent of the
// camera vendor's topic naming.
type Kind uint8

const (
	// KindUnknown is the zero value; used when a topic does not match any
	// known classification.
	KindUnknown Kind = iota
	// KindMotion covers motion detection from any vendor (e.g. AXIS
	// VideoSource/MotionAlarm, Hikvision RuleEngine/CellMotionDetector).
	KindMotion
	// KindTampering covers true tamper alarms (lens cover, scene
	// substitution). Imaging-quality alarms map to KindImageQuality.
	KindTampering
	// KindImageQuality covers VideoSource imaging alarms such as
	// ImageTooDark, ImageTooBright and ImageTooBlurry. Most integrators
	// treat these separately from tamper because they fire on legitimate
	// sunset/dawn/condensation transitions.
	KindImageQuality
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
func (k Kind) String() string {
	switch k {
	case KindUnknown:
		return "Unknown"
	case KindMotion:
		return "Motion"
	case KindTampering:
		return "Tampering"
	case KindImageQuality:
		return "ImageQuality"
	case KindDigitalInput:
		return "DigitalInput"
	case KindDigitalOutput:
		return "DigitalOutput"
	case KindObjectDetected:
		return "ObjectDetected"
	case KindAudioAlarm:
		return "AudioAlarm"
	default:
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
}

// State is the active/inactive level carried by a boolean ONVIF property
// event (e.g. IsMotion=true/false). StateUnknown is used both when the
// value cannot be parsed and when the topic is edge-triggered and carries
// no boolean state (e.g. LineDetector/Crossed).
type State uint8

const (
	StateUnknown State = iota
	StateActive
	StateInactive
)

// String implements fmt.Stringer.
func (s State) String() string {
	switch s {
	case StateUnknown:
		return "Unknown"
	case StateActive:
		return "Active"
	case StateInactive:
		return "Inactive"
	default:
		return fmt.Sprintf("State(%d)", uint8(s))
	}
}

// PropertyOperation mirrors the ONVIF wsnt:PropertyOperation attribute and
// indicates whether a message is the first sighting of a property
// (Initialized), a transition (Changed) or the property going away
// (Deleted). PropertyUnknown is used both when the attribute is absent on
// the wire (the spec allows it) and when the value is unrecognised.
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
// switch on. Topic, Source and Data preserve the original ONVIF data so
// callers can inspect the wire form without re-parsing SOAP.
//
// Source and Data are maps from ONVIF SimpleItem Name to Value because
// notifications can carry multiple items: AXIS Object Analytics for
// example emits active, classType and confidence in the same Data list,
// and standard DigitalInput notifications carry both InputToken in Source
// and LogicalState in Data.
type Event struct {
	// Kind is the normalized event category.
	Kind Kind
	// State is the active/inactive value carried by a boolean event.
	// StateUnknown for edge-triggered events (LineDetector/Crossed) that
	// carry no boolean property.
	State State
	// Operation is the ONVIF property lifecycle
	// (Initialized/Changed/Deleted).
	Operation PropertyOperation
	// DeviceID identifies the camera that produced the event. Set by the
	// Stream from the caller-supplied identifier so a single channel can
	// fan in events from multiple devices.
	DeviceID string
	// Source is the ONVIF Source SimpleItem map (e.g. InputToken,
	// VideoSourceConfigurationToken, Rule). Empty when the notification
	// has no Source section.
	Source map[string]string
	// Data is the ONVIF Data SimpleItem map (e.g. IsMotion, LogicalState,
	// active, classType). Empty when the notification has no Data
	// section.
	Data map[string]string
	// Topic is the raw ONVIF topic string, e.g.
	// tns1:VideoSource/MotionAlarm.
	Topic string
	// Timestamp is when the stream observed the event locally.
	Timestamp time.Time
	// DeviceTime is the camera-reported wsnt:UtcTime, when present and
	// parseable. Zero if the camera omits the attribute or sends an
	// unparseable value. Many cameras have drifting clocks; prefer
	// Timestamp for ordering and DeviceTime only for forensics or
	// cross-camera correlation when caller manages NTP.
	DeviceTime time.Time
	// AfterReconnect is true for events delivered after the Stream
	// silently recreated its pull-point subscription. ONVIF cameras
	// replay each property's current value with PropertyInitialized on
	// a new subscription, which would otherwise look like a flood of
	// new state changes to a consumer doing edge-detection. Watch this
	// flag to suppress duplicate handling, or treat it as a normal
	// event if you only care about steady-state level. Cleared on the
	// first event whose Operation is not PropertyInitialized.
	AfterReconnect bool
}

// Op identifies which Stream operation failed. Used by ErrPullFailed,
// ErrRenewFailed and ErrRecreateFailed so consumers can branch with
// errors.As without parsing the wrapped message.
type Op string

const (
	OpPull     Op = "pull"
	OpRenew    Op = "renew"
	OpRecreate Op = "recreate"
)

// ErrPullFailed wraps a transient PullMessages failure. The pull loop
// surfaces it on the Errors channel and continues. Consumers can match
// with errors.As(err, &stream.ErrPullFailed{}).
type ErrPullFailed struct{ Err error }

func (e ErrPullFailed) Error() string { return fmt.Sprintf("pull messages: %v", e.Err) }
func (e ErrPullFailed) Unwrap() error { return e.Err }
func (ErrPullFailed) Op() Op          { return OpPull }

// ErrRenewFailed wraps a Renew SOAP failure. Renew errors are usually
// recovered implicitly: the subscription dies, pull starts failing,
// and the reconnect logic recreates it.
type ErrRenewFailed struct{ Err error }

func (e ErrRenewFailed) Error() string { return fmt.Sprintf("renew pull point: %v", e.Err) }
func (e ErrRenewFailed) Unwrap() error { return e.Err }
func (ErrRenewFailed) Op() Op          { return OpRenew }

// ErrRecreateFailed wraps a failed CreatePullPointSubscription during
// the reconnect path. The loop continues with exponential backoff;
// consumers seeing this repeatedly should consider the camera offline.
type ErrRecreateFailed struct{ Err error }

func (e ErrRecreateFailed) Error() string { return fmt.Sprintf("recreate pull point: %v", e.Err) }
func (e ErrRecreateFailed) Unwrap() error { return e.Err }
func (ErrRecreateFailed) Op() Op          { return OpRecreate }
