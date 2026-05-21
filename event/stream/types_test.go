package stream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
		assert.Equal(t, tc.want, tc.kind.String())
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
		assert.Equal(t, tc.want, tc.state.String())
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
		assert.Equal(t, tc.want, tc.op.String())
	}
}

func TestEventZeroValue(t *testing.T) {
	var e Event
	assert.Equal(t, KindUnknown, e.Kind)
	assert.Equal(t, StateUnknown, e.State)
	assert.True(t, e.Timestamp.IsZero())
}

func TestEventTimestampPreserved(t *testing.T) {
	now := time.Now()
	e := Event{Timestamp: now}
	assert.True(t, e.Timestamp.Equal(now))
}
