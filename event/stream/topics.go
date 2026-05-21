package stream

import "strings"

// Classify maps an ONVIF topic string (e.g. "tns1:VideoSource/MotionAlarm")
// to the normalized EventKind that callers should switch on. Returns
// KindUnknown when no rule matches.
//
// The classifier strips XML-namespace prefixes (tns1:, tnsaxis:,
// tnssamsung:, ...) from each "/"-separated segment of the topic so it is
// robust to vendor namespace variants. Matching is case-sensitive because
// ONVIF topic identifiers are case-sensitive per the spec.
//
// Sources cross-checked when building the rule set below:
//   - ONVIF Topic Namespace XML
//     https://www.onvif.org/onvif/ver10/topics/topicns.xml
//   - ONVIF Analytics Service Spec (RuleEngine topics)
//     https://www.onvif.org/specs/srv/analytics/ONVIF-VideoAnalytics-Service-Spec-v220.pdf
//   - ONVIF Device IO Service Spec (DigitalInput, Relay)
//     https://www.onvif.org/specs/srv/io/ONVIF-DeviceIo-Service-Spec.pdf
//   - openvideolibs/onvif-parsers (Apache-2.0) — empirical topic table
//     extracted from Home Assistant ONVIF integration
//     https://github.com/openvideolibs/onvif-parsers
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

// canonicalizeTopic strips the XML-namespace prefix (anything up to and
// including the first ':') from each "/"-separated segment. This collapses
// vendor variants like "tns1:Device/tns1:Trigger/tns1:Relay" (Avigilon
// serialisation) and "tns1:Device/Trigger/Relay" (everyone else) to a
// single matchable form.
func canonicalizeTopic(topic string) string {
	segments := strings.Split(topic, "/")
	for i, seg := range segments {
		if idx := strings.Index(seg, ":"); idx >= 0 {
			segments[i] = seg[idx+1:]
		}
	}
	return strings.Join(segments, "/")
}

// topicRules is evaluated in order; first match wins. Keep more specific
// rules ahead of broader ones — e.g. "ObjectAnalytics/" must precede any
// future bare "Analytics" rule. Each rule cites the documentation that
// supports including it.
var topicRules = []struct {
	needle string
	kind   EventKind
}{
	// ---------- Motion -------------------------------------------------

	// tns1:VideoSource/MotionAlarm — Profile S basic motion. Emitted by
	// AXIS (basic VMD), Bosch, Dahua, Hikvision (newer firmware) and
	// Hanwha as a fallback. Data SimpleItem: State (xsd:boolean).
	// https://www.onvif.org/ver10/topics/topicns.xml
	// https://developer.axis.com/vapix/network-video/event-and-action-services/
	{"VideoSource/MotionAlarm", KindMotion},

	// tns1:VideoAnalytics/MotionAlarm — Bosch publishes motion under
	// VideoAnalytics rather than VideoSource. Data: State.
	// https://media.boschsecurity.com/fs/media/pb/media/partners_1/integration_tools_1/developer/bosch-metadata-and-iva-events.pdf
	{"VideoAnalytics/MotionAlarm", KindMotion},

	// tns1:VideoAnalytics/tnssamsung:MotionDetection — Hanwha/Samsung
	// Wisenet vendor-namespaced motion. Data: Motion ("0"/"1").
	// https://github.com/home-assistant/core/issues/66493
	{"VideoAnalytics/MotionDetection", KindMotion},

	// tns1:RuleEngine/CellMotionDetector/Motion — ONVIF Analytics
	// standard cell-motion rule. Emitted by AXIS (VMD3+), Hikvision,
	// Avigilon analytics, others. Data: IsMotion (xsd:boolean).
	// ONVIF-VideoAnalytics-Service-Spec-v220.pdf §5.3
	// https://www.hikvisioneurope.com/eu/portal/portal/Technical%20Materials/24%20How%20To/CCTV/How%20to%20solve%20third%20party%20camera%20motion%20detection%20issue.pdf
	{"CellMotionDetector/Motion", KindMotion},

	// tns1:RuleEngine/MotionRegionDetector/Motion — AXIS-specific region
	// motion rule. Data: IsMotion (xsd:boolean).
	// https://developer.axis.com/vapix/network-video/event-and-action-services/
	{"MotionRegionDetector/Motion", KindMotion},

	// ---------- Tampering ---------------------------------------------

	// tns1:RuleEngine/TamperDetector/Tamper — standard ONVIF tamper
	// rule. Data: IsTamper (xsd:boolean).
	// ONVIF-VideoAnalytics-Service-Spec-v220.pdf §5.5
	{"TamperDetector", KindTampering},

	// tns1:VideoSource/ImageTooDark|ImageTooBright|ImageTooBlurry —
	// scene-change-class signals emitted by Hikvision (and some others)
	// on firmwares without a TamperDetector rule. Treated as Tampering
	// for the purpose of normalised event routing.
	// https://www.onvif.org/ver10/topics/topicns.xml
	{"VideoSource/ImageTooDark", KindTampering},
	{"VideoSource/ImageTooBright", KindTampering},
	{"VideoSource/ImageTooBlurry", KindTampering},

	// tns1:VideoAnalytics/tnssamsung:TamperingDetection — Hanwha vendor.
	// https://github.com/home-assistant/core/issues/66493
	{"VideoAnalytics/TamperingDetection", KindTampering},

	// ---------- Digital I/O -------------------------------------------

	// tns1:Device/Trigger/DigitalInput — standard ONVIF DeviceIO topic.
	// Avigilon emits the per-segment-prefixed variant
	// "tns1:Device/tns1:Trigger/tns1:DigitalInput"; canonicalization
	// folds both to the same path. Data: LogicalState (xsd:boolean).
	// ONVIF-DeviceIo-Service-Spec.pdf §5.2
	{"Trigger/DigitalInput", KindDigitalInput},

	// tns1:Device/Trigger/Relay — standard ONVIF DeviceIO topic. Same
	// canonicalisation note as DigitalInput. Data: LogicalState.
	// ONVIF-DeviceIo-Service-Spec.pdf §5.3
	{"Trigger/Relay", KindDigitalOutput},

	// ---------- Object analytics --------------------------------------

	// tnsaxis:CameraApplicationPlatform/ObjectAnalytics/Device1Scenario<N>
	// — AXIS Object Analytics. The Scenario<N> suffix is dynamic
	// (Device1Scenario1, Device1ScenarioANY, ...) so we match the path
	// prefix. Data: active ("0"/"1").
	// https://developer.axis.com/analytics/axis-object-analytics/how-to-guides/axis-object-analytics-counting-data/
	{"ObjectAnalytics/", KindObjectDetected},

	// tns1:RuleEngine/LineDetector/Crossed — line crossing (Hikvision,
	// Bosch IVA, others). Data: ObjectId (xsd:int).
	// ONVIF-VideoAnalytics-Service-Spec-v220.pdf §5.4
	{"LineDetector/Crossed", KindObjectDetected},

	// tns1:RuleEngine/FieldDetector/ObjectsInside — intrusion / region
	// detector (Hikvision, Bosch, Dahua).
	{"FieldDetector/ObjectsInside", KindObjectDetected},

	// tns1:RuleEngine/MyRuleDetector/<RuleName> — vendor-defined rule
	// names under the ONVIF "MyRuleDetector" container. Bosch IVA and
	// Dahua SMD publish HumanDetect, VehicleDetect, ObjectsInside, etc.
	// here. We match the container so future rule names are picked up
	// automatically.
	// https://media.boschsecurity.com/fs/media/pb/media/partners_1/integration_tools_1/developer/bosch-metadata-and-iva-events.pdf
	{"MyRuleDetector/", KindObjectDetected},

	// Fallback retained for legacy ObjectsInside callers that omit the
	// MyRuleDetector container.
	{"ObjectsInside", KindObjectDetected},

	// ---------- Audio --------------------------------------------------

	// tns1:AudioAnalytics/Audio/DetectedSound — standard ONVIF audio
	// detection. Data: State (xsd:boolean).
	{"Audio/DetectedSound", KindAudioAlarm},

	// tns1:AudioSource/tnsaxis:TriggerLevel — AXIS audio level alarm.
	// https://developer.axis.com/vapix/network-video/event-and-action-services/
	{"AudioSource/TriggerLevel", KindAudioAlarm},

	// tns1:AudioAnalytics/tnssamsung:SoundDetection — Hanwha vendor.
	{"AudioAnalytics/SoundDetection", KindAudioAlarm},
}
