package stream

import "strings"

// Classify maps an ONVIF topic string (e.g. "tns1:VideoSource/MotionAlarm")
// to the normalized Kind that callers should switch on. Returns
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
func Classify(topic string) Kind {
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
//
// A segment that is only a prefix (e.g. "tns1:") canonicalizes to the
// empty string. Multiple colons in one segment are not expected in real
// ONVIF topics; the first colon wins.
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
// future bare "Analytics" rule, and "MyRuleDetector/HumanDetect" must
// precede a hypothetical broader "MyRuleDetector" entry. Each rule cites
// the documentation that supports including it.
//
// Substring matching is intentional so vendor-specific path prefixes
// outside the standard tns1: namespace (e.g.
// tnsaxis:CameraApplicationPlatform/...) still match.
//
// Note on edge-triggered topics: tns1:RuleEngine/LineDetector/Crossed
// carries an ObjectId rather than a State boolean. Consumers of Crossed
// must not expect a level-triggered Active/Inactive semantic — the Stream
// decoder will leave State as StateUnknown for these.
var topicRules = []struct {
	needle string
	kind   Kind
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

	// AXIS Guard suite — vendor analytics apps that fire motion-like
	// events with Camera<N>Profile<ID> suffixes. Treated as motion so
	// they can drive motion-triggered recording on cameras configured
	// with these apps instead of basic VMD.
	// https://developer.axis.com/vapix/applications/motion-guard
	{"CameraApplicationPlatform/MotionGuard/", KindMotion},
	{"CameraApplicationPlatform/FenceGuard/", KindMotion},
	{"CameraApplicationPlatform/LoiteringGuard/", KindMotion},

	// ---------- Tampering ---------------------------------------------

	// tns1:RuleEngine/TamperDetector/Tamper — standard ONVIF tamper
	// rule. Data: IsTamper (xsd:boolean). Anchored on the rule-name
	// segment so "TamperDetectorLog" (hypothetical) does not match.
	// ONVIF-VideoAnalytics-Service-Spec-v220.pdf §5.5
	{"TamperDetector/Tamper", KindTampering},

	// tns1:VideoSource/GlobalSceneChange/ImagingService — Hikvision (and
	// others) emit this on real lens-cover / scene substitution. This is
	// the proper tamper signal on firmwares without TamperDetector.
	// https://www.onvif.org/ver10/topics/topicns.xml
	{"GlobalSceneChange", KindTampering},

	// tns1:VideoAnalytics/tnssamsung:TamperingDetection — Hanwha vendor.
	// https://github.com/home-assistant/core/issues/66493
	{"VideoAnalytics/TamperingDetection", KindTampering},

	// ---------- Image quality -----------------------------------------

	// tns1:VideoSource/ImageTooDark|ImageTooBright|ImageTooBlurry —
	// imaging-quality alarms. Integrators (Milestone, Genetec, Frigate)
	// route these separately from tamper because they fire on legitimate
	// sunset/dawn/condensation transitions, not on actual interference.
	// https://www.onvif.org/ver10/topics/topicns.xml
	{"VideoSource/ImageTooDark", KindImageQuality},
	{"VideoSource/ImageTooBright", KindImageQuality},
	{"VideoSource/ImageTooBlurry", KindImageQuality},

	// ---------- Digital I/O -------------------------------------------

	// tns1:Device/Trigger/DigitalInput — standard ONVIF DeviceIO topic.
	// Avigilon emits the per-segment-prefixed variant
	// "tns1:Device/tns1:Trigger/tns1:DigitalInput"; canonicalization
	// folds both to the same path. Data: LogicalState (xsd:boolean),
	// Source: InputToken.
	// ONVIF-DeviceIo-Service-Spec.pdf §5.2
	{"Trigger/DigitalInput", KindDigitalInput},

	// tns1:Device/Trigger/Relay — standard ONVIF DeviceIO topic. Same
	// canonicalisation note as DigitalInput. Data: LogicalState,
	// Source: RelayToken.
	// ONVIF-DeviceIo-Service-Spec.pdf §5.3
	{"Trigger/Relay", KindDigitalOutput},

	// ---------- Object analytics --------------------------------------

	// tnsaxis:CameraApplicationPlatform/ObjectAnalytics/Device1Scenario<N>
	// — AXIS Object Analytics. Scenario suffixes are numeric per the
	// AOA configuration (Device1Scenario1, Device1Scenario2, ...). Data:
	// active ("0"/"1") plus classType / confidence when configured.
	// https://developer.axis.com/analytics/axis-object-analytics/how-to-guides/axis-object-analytics-counting-data/
	{"ObjectAnalytics/", KindObjectDetected},

	// tns1:RuleEngine/LineDetector/Crossed — line crossing (Hikvision,
	// Bosch IVA, others). Data: ObjectId (xsd:int); edge-triggered, no
	// State boolean.
	// https://www.onvif.org/specs/srv/analytics/ONVIF-VideoAnalytics-Service-Spec-v220.pdf §5.4
	{"LineDetector/Crossed", KindObjectDetected},

	// tns1:RuleEngine/FieldDetector/ObjectsInside — intrusion / region
	// detector (Hikvision, Bosch, Dahua). Data: IsInside (xsd:boolean).
	{"FieldDetector/ObjectsInside", KindObjectDetected},

	// tns1:RuleEngine/MyRuleDetector/<RuleName> — vendor-defined rule
	// names under the ONVIF MyRuleDetector container. We whitelist
	// object-class rules emitted by Bosch IVA, Dahua SMD and Hikvision
	// AcuSense so non-object rules under the same container (Bosch
	// Counter, Occupancy) do not get mis-classified.
	// https://media.boschsecurity.com/fs/media/pb/media/partners_1/integration_tools_1/developer/bosch-metadata-and-iva-events.pdf
	{"MyRuleDetector/HumanDetect", KindObjectDetected},
	{"MyRuleDetector/VehicleDetect", KindObjectDetected},
	{"MyRuleDetector/PeopleDetect", KindObjectDetected},
	{"MyRuleDetector/ObjectsInside", KindObjectDetected},
	{"MyRuleDetector/FaceDetect", KindObjectDetected},

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
