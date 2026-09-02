package vmsync

import (
	"sort"

	"github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// openBarrier locks a path after a semantic conflict. The agent that most
// recently completed a patch on the path becomes the resolver; notified
// agents are everyone with recent history on the path plus the colliding
// authors, so all affected agents hear conflict_detected and later
// conflict_resolved.
func (w *WorkspaceState) openBarrier(path string, env *vmprotocol.Envelope, conflictedWith []string) error {
	hist := w.history[path]
	resolver := ""
	resolverDevice := env.AuthorDeviceID
	if n := len(hist); n > 0 {
		resolver = hist[n-1].Agent
		if hist[n-1].Device != "" {
			resolverDevice = hist[n-1].Device
		}
	}
	notified := append([]string{}, conflictedWith...)
	// The rejected submitter is an affected party: their edit collided.
	if env.AgentSessionID != "" {
		notified = appendUnique(notified, env.AgentSessionID)
	}
	for _, p := range hist {
		if p.Agent != "" {
			notified = appendUnique(notified, p.Agent)
		}
	}
	w.barriers[path] = &barrier{
		Locked:         true,
		ResolverAgent:  resolver,
		ResolverDevice: resolverDevice,
		Notified:       notified,
		Revision:       w.seq,
	}
	return &RejectionError{
		Reason:         RejectSemanticConflict,
		ResolverAgent:  resolver,
		NotifiedAgents: notified,
	}
}

// ClearBarriersOf lifts every barrier assigned to a device that just
// lost membership (kick/leave): leaving it bound to the evicted resolver
// would block the path for every remaining member until dissolution.
// Returns the affected paths so callers can notify the room.
func (w *WorkspaceState) ClearBarriersOf(deviceID string) []string {
	var cleared []string
	for path, b := range w.barriers {
		if b.Locked && b.ResolverDevice == deviceID {
			b.Locked = false
			cleared = append(cleared, path)
		}
	}
	sort.Strings(cleared)
	return cleared
}

// ResolveBarrier lifts the barrier after the resolver committed a fixed
// revision. Returns the notified agents so the caller broadcasts
// conflict_resolved with the resolved revision.
func (w *WorkspaceState) ResolveBarrier(path string) (notified []string, ok bool) {
	b := w.barriers[path]
	if b == nil || !b.Locked {
		return nil, false
	}
	notified = b.Notified
	delete(w.barriers, path)
	return notified, true
}

// BarrierInfo exposes barrier state for status and events.
func (w *WorkspaceState) BarrierInfo(path string) (resolver string, locked bool, notified []string) {
	b := w.barriers[path]
	if b == nil {
		return "", false, nil
	}
	return b.ResolverAgent, b.Locked, b.Notified
}
