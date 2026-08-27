package vmprotocol

import (
	"errors"
	"fmt"
	"strings"
)

// TLD is the reserved virtual-host suffix of the OpenTuttiVM room network.
// It resolves only inside the Tutti Browser and agent/terminal session
// containers; the host OS, system browsers, and host terminals never see it.
const TLD = "tutti"

// ReservedCIDR is the synthetic address block backing the .tutti namespace.
// Addresses are never routed on physical networks; room-sync gateways map
// them onto (device, session, port) routes through the server tunnel.
const ReservedCIDR = "100.96.0.0/12"

// TuttiHost is a parsed .tutti virtual hostname.
type TuttiHost struct {
	// Device is the slug of the target device, e.g. "annasmacbookpro".
	Device string
	// Session is the optional session label, e.g. "claude-a". Empty means a
	// device-level address.
	Session string
}

// String renders the canonical hostname.
func (h TuttiHost) String() string {
	if h.Session == "" {
		return h.Device + "." + TLD
	}
	return h.Session + "." + h.Device + "." + TLD
}

// IsSessionLevel reports whether the address pins one session.
func (h TuttiHost) IsSessionLevel() bool { return h.Session != "" }

// ParseTuttiHost parses "<device>.tutti" or "<session>.<device>.tutti".
// Session and device slugs are lowercase letters, digits, and hyphens.
func ParseTuttiHost(host string) (TuttiHost, error) {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if !strings.HasSuffix(host, "."+TLD) {
		return TuttiHost{}, fmt.Errorf("not a .%s host: %q", TLD, host)
	}
	body := strings.TrimSuffix(host, "."+TLD)
	parts := strings.Split(body, ".")
	if len(parts) > 2 {
		return TuttiHost{}, fmt.Errorf("too many labels in .%s host: %q", TLD, host)
	}
	for _, p := range parts {
		if !isValidSlug(p) {
			return TuttiHost{}, fmt.Errorf("invalid .%s label %q", TLD, p)
		}
	}
	h := TuttiHost{Device: parts[len(parts)-1]}
	if len(parts) == 2 {
		h.Session = parts[0]
	}
	return h, nil
}

func isValidSlug(s string) bool {
	if s == "" || strings.HasPrefix(s, "-") || strings.HasSuffix(s, "-") {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
		default:
			return false
		}
	}
	return true
}

// SlugifyHostname converts a machine name into a valid .tutti slug.
func SlugifyHostname(name string) string {
	var b strings.Builder
	lastDash := true // avoid leading dash
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "device"
	}
	return out
}

// RouteResolution is the outcome of resolving a device-level address.
type RouteResolution struct {
	// Resolved is true when the route is unambiguous.
	Resolved bool
	// SessionID pins the single target session when Resolved.
	SessionID string
	// SessionLabel is the human-facing label of SessionID.
	SessionLabel string
	// Candidates lists all sessions occupying the port when ambiguous,
	// ordered deterministically by session label.
	Candidates []SessionCandidate
}

// SessionCandidate describes one session occupying a device-level port.
type SessionCandidate struct {
	SessionID    string `json:"session_id"`
	SessionLabel string `json:"session_label"`
	Agent        string `json:"agent,omitempty"`
	// CanonicalHost is the always-unique full session hostname including the
	// port, suitable for links and agent-facing hints.
	CanonicalHost string `json:"canonical_host"`
}

// ErrAmbiguousRoute is returned for raw-TCP access to a device-level address
// whose port is occupied by multiple sessions. Raw TCP cannot render an H5
// selector, so callers must use the full session hostname.
var ErrAmbiguousRoute = errors.New("ambiguous .tutti route: multiple sessions occupy the port; use the full session hostname (session.device.tutti:port)")

// ResolveDeviceRoute applies the room addressing rules to a device-level
// (device, port) lookup over the occupied sessions:
//
//   - zero sessions: unresolved
//   - one session: transparently route to it (the short alias)
//   - multiple sessions: callers decide by protocol — HTTP(S) renders the H5
//     session selector, raw TCP fails with ErrAmbiguousRoute
func ResolveDeviceRoute(port int, occupied []SessionCandidate) (RouteResolution, error) {
	matches := make([]SessionCandidate, 0, len(occupied))
	for _, c := range occupied {
		matches = append(matches, c)
	}
	SortCandidates(matches)
	res := RouteResolution{Candidates: matches}
	switch len(matches) {
	case 0:
		return res, nil
	case 1:
		res.Resolved = true
		res.SessionID = matches[0].SessionID
		res.SessionLabel = matches[0].SessionLabel
		return res, nil
	default:
		return res, ErrAmbiguousRoute
	}
}

// SortCandidates orders candidates deterministically by canonical host.
func SortCandidates(c []SessionCandidate) {
	for i := 1; i < len(c); i++ {
		for j := i; j > 0 && c[j].CanonicalHost < c[j-1].CanonicalHost; j-- {
			c[j], c[j-1] = c[j-1], c[j]
		}
	}
}
