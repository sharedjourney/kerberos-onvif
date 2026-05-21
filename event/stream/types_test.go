package stream

import (
	"testing"
	"time"
)

func TestEventKindString(t *testing.T) {
	tests := []struct {
		kind EventKind
		want string
	}{
		{KindUnknown, "Unknown"},
		{KindMotion, "Motion"},
		{KindTampering, "Tampering"},
		{KindDigitalInput, "DigitalInput"},
		{KindDigitalOutput, "DigitalOutput"},
		{KindObjectDetected, "ObjectDetected"},
		{KindAudioAlarm, "AudioAlarm"},
		{EventKind(255), "EventKind(255)"},
	}
	for _, tc := range tests {
		if got := tc.kind.String(); got != tc.want {
			t.Errorf("EventKind(%d).String() = %q, want %q", tc.kind, got, tc.want)
		}
	}
}

func TestEventStateString(t *testing.T) {
	tests := []struct {
		state EventState
		want  string
	}{
		{StateUnknown, "Unknown"},
		{StateActive, "Active"},
		{StateInactive, "Inactive"},
		{EventState(255), "EventState(255)"},
	}
	for _, tc := range tests {
		if got := tc.state.String(); got != tc.want {
			t.Errorf("EventState(%d).String() = %q, want %q", tc.state, got, tc.want)
		}
	}
}

func TestPropertyOperationString(t *testing.T) {
	tests := []struct {
		op   PropertyOperation
		want string
	}{
		{PropertyUnknown, "Unknown"},
		{PropertyInitialized, "Initialized"},
		{PropertyChanged, "Changed"},
		{PropertyDeleted, "Deleted"},
		{PropertyOperation(255), "PropertyOperation(255)"},
	}
	for _, tc := range tests {
		if got := tc.op.String(); got != tc.want {
			t.Errorf("PropertyOperation(%d).String() = %q, want %q", tc.op, got, tc.want)
		}
	}
}

func TestEventZeroValue(t *testing.T) {
	var e Event
	if e.Kind != KindUnknown {
		t.Errorf("zero Event.Kind = %v, want KindUnknown", e.Kind)
	}
	if e.State != StateUnknown {
		t.Errorf("zero Event.State = %v, want StateUnknown", e.State)
	}
	if !e.Timestamp.IsZero() {
		t.Errorf("zero Event.Timestamp = %v, want zero time", e.Timestamp)
	}
}

func TestEventTimestampPreserved(t *testing.T) {
	now := time.Now()
	e := Event{Timestamp: now}
	if !e.Timestamp.Equal(now) {
		t.Errorf("Event.Timestamp = %v, want %v", e.Timestamp, now)
	}
}
