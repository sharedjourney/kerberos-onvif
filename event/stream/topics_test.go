package stream

import "testing"

func TestClassifyTopic(t *testing.T) {
	tests := []struct {
		name  string
		topic string
		want  EventKind
	}{
		// AXIS: motion alarm on video source.
		{"axis_video_source_motion", "tns1:VideoSource/MotionAlarm", KindMotion},
		// AXIS / Hikvision / others: ONVIF cell-motion detector rule.
		{"cell_motion_detector", "tns1:RuleEngine/CellMotionDetector/Motion", KindMotion},
		// AXIS region motion.
		{"motion_region_detector", "tns1:RuleEngine/MotionRegionDetector/Motion", KindMotion},
		// Vendor-prefixed motion (e.g. Bosch).
		{"bosch_motion", "tns1:VideoAnalytics/tnsbosch:MotionAlarm", KindMotion},
		// Tampering / scene change.
		{"tamper_detector", "tns1:RuleEngine/TamperDetector/Tamper", KindTampering},
		{"axis_scene_tamper", "tns1:VideoSource/ImageTooDark/ImagingService", KindUnknown}, // not classified
		// Digital input — vendor-namespaced variant from agent code.
		{"digital_input", "tns1:Device/Trigger/DigitalInput", KindDigitalInput},
		{"digital_input_samsung", "tns1:Device/tns1:Trigger/tnssamsung:DigitalInput", KindDigitalInput},
		// Digital output / relay.
		{"relay", "tns1:Device/Trigger/Relay", KindDigitalOutput},
		{"relay_avigilon", "tns1:Device/tns1:Trigger/tns1:Relay", KindDigitalOutput},
		// Analytics object detection (AXIS object analytics, generic motion analytics).
		{"object_detected", "tns1:RuleEngine/MyRuleDetector/ObjectsInside", KindObjectDetected},
		{"axis_object_analytics", "tnsaxis:CameraApplicationPlatform/ObjectAnalytics/Device1ScenarioANY", KindObjectDetected},
		// Audio.
		{"audio_alarm", "tns1:AudioAnalytics/Audio/DetectedSound", KindAudioAlarm},
		{"axis_audio", "tnsaxis:AudioSource/TriggerLevel", KindAudioAlarm},
		// Unknown — should not be force-classified.
		{"empty", "", KindUnknown},
		{"unknown_topic", "tns1:UserAlarm/IVA", KindUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.topic); got != tc.want {
				t.Errorf("Classify(%q) = %v, want %v", tc.topic, got, tc.want)
			}
		})
	}
}

func TestClassifyIsCaseSensitive(t *testing.T) {
	// ONVIF topic names are case-sensitive per spec; we should not silently
	// upper/lower-case. A lowercased topic must not match.
	if got := Classify("tns1:videosource/motionalarm"); got != KindUnknown {
		t.Errorf("Classify lowercase = %v, want KindUnknown (topics are case-sensitive)", got)
	}
}
