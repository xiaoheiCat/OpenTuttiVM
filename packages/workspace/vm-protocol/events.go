package vmprotocol

import "encoding/json"

// EventTopic identifies a room realtime event carried on the business
// WebSocket. Topics are the schema-first contract between the server and all
// room clients.
type EventTopic string

const (
	// TopicPresence announces devices joining, leaving, or changing agent
	// availability in the room.
	TopicPresence EventTopic = "room.presence"
	// TopicOperation carries accepted workspace operations with their
	// assigned server sequence to every connected device.
	TopicOperation EventTopic = "workspace.operation"
	// TopicOperationRejected tells one author their submission was fenced by
	// a conflict barrier or optimistic version check.
	TopicOperationRejected EventTopic = "workspace.operation_rejected"
	// TopicConflictDetected announces that a path entered the conflict
	// barrier and which agent sessions were notified.
	TopicConflictDetected EventTopic = "workspace.conflict_detected"
	// TopicConflictResolved announces a resolver committed a fixed revision
	// and the barrier lifted; includes the resolved revision so blocked
	// agents can resync before continuing.
	TopicConflictResolved EventTopic = "workspace.conflict_resolved"
	// TopicSnapshotAnnounce tells devices a checkpoint exists and old
	// operations may be compacted away.
	TopicSnapshotAnnounce EventTopic = "workspace.snapshot"
	// TopicEnvironmentChanged announces that environment definition files
	// (.opentuttivm/Dockerfile, .devcontainer/devcontainer.json) changed.
	// Devices surface "Environment changed — Rebuild" independently; the
	// server never rebuilds local runtimes.
	TopicEnvironmentChanged EventTopic = "room.environment.changed"
	// Agent Borrowing topics (agent.shared, agent.borrow_command,
	// agent.borrow_revoked, agent.approval_request,
	// agent.approval_decision) live in packages/workspace/vm-agent: the
	// borrowing contract is its own seam, not workspace synchronization.

	// TopicPortsChanged announces TCP ports observed listening in an agent or
	// terminal session, feeding the room preview registry.
	TopicPortsChanged EventTopic = "room.ports_changed"
	// TopicOwnerLost announces the owner disappeared past the grace
	// period while no full-replica successor is online: the room waits;
	// members should run an explicit transfer (whose readiness phase
	// materializes the candidate) or bring a full replica online.
	TopicOwnerLost EventTopic = "room.owner_lost"
	// TopicRoomEnding announces the room is ending (last leave, disband, or
	// server shutdown) so devices can run their local finalize path.
	TopicRoomEnding EventTopic = "room.ending"
)

// Event is the envelope for all business WebSocket events.
type Event struct {
	Topic   EventTopic      `json:"topic"`
	RoomID  string          `json:"room_id"`
	Payload json.RawMessage `json:"payload"`
}

// PresenceDevice is the payload of room.presence events.
type PresenceDevice struct {
	DeviceID    string `json:"device_id"`
	DisplayName string `json:"display_name"`
	Hostname    string `json:"hostname"`
	Online      bool   `json:"online"`
	ConnectedAt int64  `json:"connected_at,omitempty"`
	IsOwner     bool   `json:"is_owner"`
}

// EnvironmentChangedPayload is the payload of room.environment.changed.
type EnvironmentChangedPayload struct {
	Revision     int64    `json:"revision"`
	ChangedFiles []string `json:"changed_files"`
	ChangedBy    string   `json:"changed_by"`
}

// PortsChangedPayload is the payload of room.ports_changed. Ports are
// room-visible by default; the room trusts participants.
type PortsChangedPayload struct {
	DeviceID  string `json:"device_id"`
	SessionID string `json:"session_id"`
	// SessionLabel is the human-facing session name used by selectors.
	SessionLabel string `json:"session_label"`
	Port         int    `json:"port"`
	Protocol     string `json:"protocol"`
	Listening    bool   `json:"listening"`
}

// ConflictPayload is shared by conflict_detected and conflict_resolved.
type ConflictPayload struct {
	Path             string   `json:"path"`
	ResolverAgent    string   `json:"resolver_agent,omitempty"`
	NotifiedAgents   []string `json:"notified_agents,omitempty"`
	ResolvedRevision uint64   `json:"resolved_revision,omitempty"`
	ConflictRevision uint64   `json:"conflict_revision,omitempty"`
}

// RejectionPayload tells an author why an operation was fenced.
type RejectionPayload struct {
	OperationID string `json:"operation_id"`
	Path        string `json:"path"`
	Reason      string `json:"reason"`
	// CurrentHash lets blob authors re-upload against the latest state.
	CurrentHash string `json:"current_hash,omitempty"`
}
