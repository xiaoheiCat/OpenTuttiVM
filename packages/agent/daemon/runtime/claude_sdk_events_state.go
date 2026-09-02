package agentruntime

import (
	"strings"

	activityshared "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/daemon/activity/events"
)

func (a *ClaudeCodeSDKAdapter) takeClaudeSDKResponseWaiter(adapterSession *claudeSDKAdapterSession, event claudeSDKSidecarEvent) chan claudeSDKSidecarEvent {
	if a == nil || adapterSession == nil || strings.TrimSpace(event.ID) == "" {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	response := adapterSession.pendingResponses[strings.TrimSpace(event.ID)]
	if response != nil {
		delete(adapterSession.pendingResponses, strings.TrimSpace(event.ID))
	}
	return response
}

func (a *ClaudeCodeSDKAdapter) registerClaudeSDKResponse(adapterSession *claudeSDKAdapterSession, requestID string) chan claudeSDKSidecarEvent {
	response := make(chan claudeSDKSidecarEvent, 1)
	a.mu.Lock()
	if adapterSession.pendingResponses == nil {
		adapterSession.pendingResponses = make(map[string]chan claudeSDKSidecarEvent)
	}
	adapterSession.pendingResponses[strings.TrimSpace(requestID)] = response
	a.mu.Unlock()
	return response
}

func (a *ClaudeCodeSDKAdapter) unregisterClaudeSDKResponse(adapterSession *claudeSDKAdapterSession, requestID string, response chan claudeSDKSidecarEvent) {
	if a == nil || adapterSession == nil || response == nil {
		return
	}
	a.mu.Lock()
	if current := adapterSession.pendingResponses[strings.TrimSpace(requestID)]; current == response {
		delete(adapterSession.pendingResponses, strings.TrimSpace(requestID))
	}
	a.mu.Unlock()
}

func (a *ClaudeCodeSDKAdapter) claudeSDKSessionSnapshot(adapterSession *claudeSDKAdapterSession) Session {
	if a == nil || adapterSession == nil {
		return Session{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return adapterSession.session
}

func (a *ClaudeCodeSDKAdapter) updateClaudeSDKSessionSnapshot(adapterSession *claudeSDKAdapterSession, events []activityshared.Event) {
	if a == nil || adapterSession == nil || len(events) == 0 {
		return
	}
	a.mu.Lock()
	adapterSession.session = applySessionEvents(adapterSession.session, eventsOwnedBySession(events, adapterSession.session.AgentSessionID))
	a.mu.Unlock()
}

func (a *ClaudeCodeSDKAdapter) emitClaudeSDKSessionEvents(agentSessionID string, events []activityshared.Event) {
	if a == nil || len(events) == 0 {
		return
	}
	a.mu.Lock()
	sink := a.eventSink
	a.mu.Unlock()
	if sink != nil {
		sink(agentSessionID, events)
	}
}
