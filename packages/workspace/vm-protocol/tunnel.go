package vmprotocol

import "encoding/json"

// TunnelAction selects what a tunnel stream does.
type TunnelAction string

const (
	// TunnelConnect asks the peer to relay the stream to a room route.
	TunnelConnect TunnelAction = "connect"
	// TunnelBind registers the local end as reachable for a route family; the
	// server uses device tunnel connections as the dial target for room
	// routes.
	TunnelBind TunnelAction = "bind"
)

// TunnelHeader is the first JSON frame on every tunnel stream. It identifies
// the logical route so the server relay can stitch device A to device B
// without a per-route WebSocket.
type TunnelHeader struct {
	Action TunnelAction `json:"action"`
	RoomID string       `json:"room_id"`
	// DeviceID identifies the connecting device (from session auth).
	DeviceID string `json:"device_id"`
	// Route targets a room preview route for connect actions.
	Route RouteKey `json:"route"`
}

// RouteKey is the canonical preview route identity: ports belong to a device
// and session, never to the room globally.
type RouteKey struct {
	RoomID    string `json:"room_id"`
	DeviceID  string `json:"device_id"`
	SessionID string `json:"session_id"`
	Port      int    `json:"port"`
}

// LiveRoute is one advertised route plus the server-computed identity
// pieces a gateway cannot derive locally: raw device ids may differ from
// the enrolled-hostname slugs canonical .tutti hosts use, so the gateway
// binds and dials with BOTH (slug for hostnames, raw id for lookups and
// tunnel targets).
type LiveRoute struct {
	RouteKey
	DeviceSlug    string `json:"device_slug,omitempty"`
	CanonicalHost string `json:"canonical_host,omitempty"`
}

// Encode serializes the header for the first tunnel frame.
func (h TunnelHeader) Encode() ([]byte, error) { return json.Marshal(h) }

// DecodeTunnelHeader parses the first tunnel frame.
func DecodeTunnelHeader(data []byte) (TunnelHeader, error) {
	var h TunnelHeader
	err := json.Unmarshal(data, &h)
	return h, err
}
