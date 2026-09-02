package agentruntime

import activityshared "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/daemon/activity/events"

func markClaudeSDKContextHandoffRequired(
	event *activityshared.Event,
	payload map[string]any,
) {
	if event == nil || !payloadBoolValue(payload, "contextHandoffRequired") {
		return
	}
	event.Payload.Metadata["noticeKind"] = "context_handoff_required"
	event.Payload.Metadata["severity"] = "error"
	event.Payload.Metadata["retryable"] = false
}
