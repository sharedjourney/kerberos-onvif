package stream

import "strings"

// Classify maps an ONVIF topic string (e.g. "tns1:VideoSource/MotionAlarm")
// to the normalized EventKind that callers should switch on. Returns
// KindUnknown when no rule matches.
//
// The classifier strips XML-namespace prefixes from each path segment so it
// is robust to vendor-specific namespaces like tnsaxis:, tnsbosch:,
// tnssamsung:. Matching is case-sensitive because ONVIF topic identifiers
// are case-sensitive per the spec.
func Classify(topic string) EventKind {
	if topic == "" {
		return KindUnknown
	}
	canonical := canonicalizeTopic(topic)
	for _, rule := range topicRules {
		if strings.Contains(canonical, rule.needle) {
			return rule.kind
		}
	}
	return KindUnknown
}

// canonicalizeTopic strips the XML-namespace prefix (e.g. "tns1:") from each
// "/"-separated segment of the topic. This collapses vendor variants like
// "tns1:Device/tnssamsung:DigitalInput" and the plain
// "tns1:Device/DigitalInput" form to the same canonical path.
func canonicalizeTopic(topic string) string {
	segments := strings.Split(topic, "/")
	for i, seg := range segments {
		if idx := strings.Index(seg, ":"); idx >= 0 {
			segments[i] = seg[idx+1:]
		}
	}
	return strings.Join(segments, "/")
}

// topicRules is evaluated in order; first match wins. Keep the most specific
// rules first when adding new entries — e.g. "MotionDetector/Motion" must
// precede a hypothetical bare "/Motion" rule.
var topicRules = []struct {
	needle string
	kind   EventKind
}{
	// Motion: covers AXIS VideoSource/MotionAlarm, ONVIF
	// RuleEngine/CellMotionDetector and MotionRegionDetector, and
	// vendor-namespaced MotionAlarm variants (Bosch).
	{"MotionAlarm", KindMotion},
	{"CellMotionDetector/Motion", KindMotion},
	{"MotionRegionDetector/Motion", KindMotion},

	// Tampering: ONVIF RuleEngine/TamperDetector.
	{"TamperDetector", KindTampering},

	// Digital I/O: ONVIF Device/Trigger/{DigitalInput,Relay}. The
	// canonicalization step normalizes vendor-prefixed inner segments
	// (tnssamsung:DigitalInput, tns1:Relay) to the bare names.
	{"Trigger/DigitalInput", KindDigitalInput},
	{"Trigger/Relay", KindDigitalOutput},

	// Object analytics: AXIS ObjectAnalytics scenarios use dynamic suffixes
	// (Device1ScenarioANY, Device1Scenario1, ...), so match the path prefix.
	{"ObjectAnalytics/", KindObjectDetected},
	{"ObjectsInside", KindObjectDetected},

	// Audio: ONVIF AudioAnalytics/Audio/DetectedSound and AXIS
	// AudioSource/TriggerLevel.
	{"Audio/DetectedSound", KindAudioAlarm},
	{"AudioSource/TriggerLevel", KindAudioAlarm},
}
