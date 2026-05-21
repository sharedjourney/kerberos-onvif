package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClassifyTopic(t *testing.T) {
	tests := []struct {
		name  string
		topic string
		want  EventKind
	}{
		// --- Motion -----------------------------------------------------

		// Profile S basic motion (AXIS basic VMD, Bosch, Dahua,
		// Hikvision newer firmware, Hanwha fallback). Data: State.
		{"video_source_motion_alarm", "tns1:VideoSource/MotionAlarm", KindMotion},

		// ONVIF Analytics rule (AXIS, Hikvision standard, Avigilon
		// analytics). Data: IsMotion.
		{"cell_motion_detector", "tns1:RuleEngine/CellMotionDetector/Motion", KindMotion},

		// AXIS region rule. Data: IsMotion.
		{"motion_region_detector", "tns1:RuleEngine/MotionRegionDetector/Motion", KindMotion},

		// Bosch publishes motion under VideoAnalytics (not VideoSource).
		{"bosch_video_analytics_motion", "tns1:VideoAnalytics/MotionAlarm", KindMotion},

		// Hanwha (Samsung/Wisenet) vendor-namespaced motion.
		{"hanwha_samsung_motion", "tns1:VideoAnalytics/tnssamsung:MotionDetection", KindMotion},

		// --- Tampering / scene change ----------------------------------

		// ONVIF RuleEngine tamper rule. Data: IsTamper.
		{"tamper_detector", "tns1:RuleEngine/TamperDetector/Tamper", KindTampering},

		// Hikvision uses VideoSource/Image* topics for tamper-class
		// signals on firmwares without TamperDetector.
		{"hikvision_image_too_dark", "tns1:VideoSource/ImageTooDark/ImagingService", KindTampering},
		{"hikvision_image_too_bright", "tns1:VideoSource/ImageTooBright/ImagingService", KindTampering},
		{"hikvision_image_too_blurry", "tns1:VideoSource/ImageTooBlurry/ImagingService", KindTampering},

		// Hanwha vendor-namespaced tampering.
		{"hanwha_tampering", "tns1:VideoAnalytics/tnssamsung:TamperingDetection", KindTampering},

		// --- Digital input ---------------------------------------------

		// Standard ONVIF Device IO topic — same across all vendors that
		// follow the spec.
		{"digital_input", "tns1:Device/Trigger/DigitalInput", KindDigitalInput},

		// Avigilon serialises every path segment with a namespace prefix.
		{"digital_input_avigilon", "tns1:Device/tns1:Trigger/tns1:DigitalInput", KindDigitalInput},

		// --- Digital output / relay ------------------------------------

		{"relay", "tns1:Device/Trigger/Relay", KindDigitalOutput},
		{"relay_avigilon", "tns1:Device/tns1:Trigger/tns1:Relay", KindDigitalOutput},

		// --- Object analytics ------------------------------------------

		// AXIS Object Analytics scenarios — the suffix is dynamic
		// (Device1Scenario1, Device1ScenarioANY, ...).
		{"axis_object_analytics_scenario_any", "tnsaxis:CameraApplicationPlatform/ObjectAnalytics/Device1ScenarioANY", KindObjectDetected},
		{"axis_object_analytics_scenario_1", "tnsaxis:CameraApplicationPlatform/ObjectAnalytics/Device1Scenario1", KindObjectDetected},

		// Hikvision line crossing.
		{"line_detector_crossed", "tns1:RuleEngine/LineDetector/Crossed", KindObjectDetected},

		// Region / intrusion detector.
		{"field_detector_objects_inside", "tns1:RuleEngine/FieldDetector/ObjectsInside", KindObjectDetected},

		// Bosch IVA / Dahua SMD publish vendor rule names under
		// MyRuleDetector.
		{"my_rule_detector_human", "tns1:RuleEngine/MyRuleDetector/HumanDetect", KindObjectDetected},
		{"my_rule_detector_vehicle", "tns1:RuleEngine/MyRuleDetector/VehicleDetect", KindObjectDetected},
		{"my_rule_detector_objects_inside", "tns1:RuleEngine/MyRuleDetector/ObjectsInside", KindObjectDetected},

		// --- Audio -----------------------------------------------------

		{"audio_detected_sound", "tns1:AudioAnalytics/Audio/DetectedSound", KindAudioAlarm},
		{"axis_audio_trigger_level", "tns1:AudioSource/tnsaxis:TriggerLevel", KindAudioAlarm},
		{"hanwha_sound_detection", "tns1:AudioAnalytics/tnssamsung:SoundDetection", KindAudioAlarm},

		// --- Negative cases --------------------------------------------

		{"empty", "", KindUnknown},
		{"unknown_topic", "tns1:UserAlarm/IVA", KindUnknown},
		{"unrelated_recording_config", "tns1:RecordingConfig/JobState", KindUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Classify(tc.topic), "topic=%q", tc.topic)
		})
	}
}

func TestClassifyIsCaseSensitive(t *testing.T) {
	// ONVIF topic identifiers are case-sensitive per the spec; a
	// lowercased topic must not match a capitalised pattern.
	assert.Equal(t, KindUnknown, Classify("tns1:videosource/motionalarm"))
}

func TestCanonicalizeTopicStripsNamespaces(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"tns1:VideoSource/MotionAlarm", "VideoSource/MotionAlarm"},
		{"tns1:Device/tns1:Trigger/tns1:Relay", "Device/Trigger/Relay"},
		{"tns1:VideoAnalytics/tnssamsung:MotionDetection", "VideoAnalytics/MotionDetection"},
		{"tnsaxis:CameraApplicationPlatform/ObjectAnalytics/Device1Scenario1", "CameraApplicationPlatform/ObjectAnalytics/Device1Scenario1"},
		{"", ""},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, canonicalizeTopic(tc.in), "input=%q", tc.in)
	}
}
