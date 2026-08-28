// Package preview maintains the room preview route registry: which agent or
// terminal session listens on which (room, device, session, port) route.
// Any listening TCP port is room-visible by default; the room trusts its
// participants.
package preview

import (
	"sort"
	"sync"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// Entry is one registered listening port.
type Entry struct {
	vmprotocol.RouteKey
	SessionLabel string
	Agent        string
	Protocol     string
	DeviceSlug   string
}

// Registry indexes routes by room.
type Registry struct {
	mu sync.RWMutex
	// routes[roomID][deviceID] holds the device's session labels for
	// candidate rendering.
	routes map[string]map[string]map[vmprotocol.RouteKey]Entry
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{routes: map[string]map[string]map[vmprotocol.RouteKey]Entry{}}
}

// Upsert registers or refreshes one route.
func (r *Registry) Upsert(e Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.routes[e.RoomID] == nil {
		r.routes[e.RoomID] = map[string]map[vmprotocol.RouteKey]Entry{}
	}
	if r.routes[e.RoomID][e.DeviceID] == nil {
		r.routes[e.RoomID][e.DeviceID] = map[vmprotocol.RouteKey]Entry{}
	}
	r.routes[e.RoomID][e.DeviceID][e.RouteKey] = e
}

// Remove drops one route (port stopped listening or session ended).
func (r *Registry) Remove(key vmprotocol.RouteKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if devs := r.routes[key.RoomID]; devs != nil {
		if sess := devs[key.DeviceID]; sess != nil {
			delete(sess, key)
		}
	}
}

// DeviceRoutes returns every session occupying (room, device, port),
// ordered by canonical host — the input to the device-level short-address
// rules: unique occupancy routes transparently, ambiguous HTTP gets the H5
// selector, ambiguous raw TCP must use the full session hostname.
func (r *Registry) DeviceRoutes(roomID, deviceID string, port int) []vmprotocol.SessionCandidate {
	r.mu.RLock()
	defer r.mu.RUnlock()
	devs := r.routes[roomID]
	if devs == nil {
		return nil
	}
	out := []vmprotocol.SessionCandidate{}
	for key, e := range devs[deviceID] {
		if key.Port != port {
			continue
		}
		out = append(out, vmprotocol.SessionCandidate{
			SessionID:     key.SessionID,
			SessionLabel:  e.SessionLabel,
			Agent:         e.Agent,
			CanonicalHost: canonicalHost(e),
		})
	}
	vmprotocol.SortCandidates(out)
	return out
}

func canonicalHost(e Entry) string {
	return e.SessionLabel + "." + e.DeviceSlug + ".tutti:" + itoa(e.Port)
}

// SessionRoute returns the route key for a full session-level address.
func (r *Registry) SessionRoute(roomID, deviceID, sessionID string, port int) (Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := vmprotocol.RouteKey{RoomID: roomID, DeviceID: deviceID, SessionID: sessionID, Port: port}
	e, ok := r.routes[roomID][deviceID][key]
	return e, ok
}

// RoomSessions lists every registered session of one room (device slug
// included) for presence-style rendering.
func (r *Registry) RoomSessions(roomID string) []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Entry
	for _, devs := range r.routes[roomID] {
		for _, e := range devs {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SessionLabel < out[j].SessionLabel })
	return out
}

// HasRoute reports whether (device, session, port) is currently
// advertised in the room — the relay's dial authorization.
func (r *Registry) HasRoute(roomID, deviceID, sessionID string, port int) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := vmprotocol.RouteKey{RoomID: roomID, DeviceID: deviceID, SessionID: sessionID, Port: port}
	_, ok := r.routes[roomID][deviceID][key]
	return ok
}

// ClearRoom drops a dissolved room's registry.
func (r *Registry) ClearRoom(roomID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.routes, roomID)
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
