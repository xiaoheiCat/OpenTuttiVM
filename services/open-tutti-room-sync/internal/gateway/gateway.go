// Package gateway resolves .tutti virtual hostnames onto room preview
// routes and enforces the addressing rules:
//
//   - session addresses (session.device.tutti:port) always route
//   - device short addresses (device.tutti:port) route when unambiguous
//   - ambiguous device addresses: HTTP(S) renders the H5 session selector;
//     raw TCP fails with a clear error — no random choice
//
// The gateway also allocates synthetic IPs from 100.96.0.0/12 backing each
// hostname so the container network can intercept traffic deterministically.
// The block never appears on physical networks.
package gateway

import (
	"fmt"
	"net"
	"strings"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// RouteLookup queries the server route registry.
type RouteLookup interface {
	// LookupDevice returns the sessions occupying (device, port).
	LookupDevice(deviceSlug string, port int) ([]vmprotocol.SessionCandidate, error)
	// LookupSession resolves a full session address.
	LookupSession(deviceSlug, sessionID string, port int) (vmprotocol.SessionCandidate, error)
}

// Resolution is a resolved target route.
type Resolution struct {
	DeviceSlug string
	SessionID  string
	Port       int
	// Candidates is non-nil when the address was ambiguous and the caller
	// must render an H5 selector (HTTP) or fail (raw TCP).
	Candidates []vmprotocol.SessionCandidate
}

// Resolve applies the addressing rules to hostport ("host:port").
func Resolve(lookup RouteLookup, hostport string) (Resolution, error) {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return Resolution{}, fmt.Errorf("parse %q: %w", hostport, err)
	}
	portNum := 0
	fmt.Sscanf(port, "%d", &portNum)
	if portNum == 0 {
		return Resolution{}, fmt.Errorf("invalid port in %q", hostport)
	}
	parsed, err := vmprotocol.ParseTuttiHost(host)
	if err != nil {
		return Resolution{}, err
	}

	if parsed.IsSessionLevel() {
		cand, err := lookup.LookupSession(parsed.Device, sessionIDFromLabel(parsed.Session), portNum)
		if err != nil {
			return Resolution{}, err
		}
		return Resolution{DeviceSlug: parsed.Device, SessionID: cand.SessionID, Port: portNum}, nil
	}

	candidates, err := lookup.LookupDevice(parsed.Device, portNum)
	if err != nil {
		return Resolution{}, err
	}
	res, resolveErr := vmprotocol.ResolveDeviceRoute(portNum, candidates)
	if resolveErr != nil {
		// Ambiguous: return candidates so HTTP callers render the selector;
		// raw-TCP callers surface the error themselves.
		return Resolution{DeviceSlug: parsed.Device, Port: portNum, Candidates: res.Candidates}, resolveErr
	}
	if !res.Resolved {
		return Resolution{}, fmt.Errorf("no session listens on %s", hostport)
	}
	return Resolution{DeviceSlug: parsed.Device, SessionID: res.SessionID, Port: portNum}, nil
}

// sessionIDFromLabel maps a session label to its registry id; the label is
// the registry key used by candidate listing, so v1 uses the label
// verbatim. Room-sync labels carry the mapping when registering ports.
func sessionIDFromLabel(label string) string { return "sess-" + label }

// SessionSelectorPage renders the minimal H5 picker for an ambiguous HTTPS
// device address. The local CA terminates TLS so the picker can appear
// before the origin responds.
func SessionSelectorPage(candidates []vmprotocol.SessionCandidate) string {
	var b strings.Builder
	b.WriteString(`<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Choose a session</title>
<style>body{font-family:system-ui,sans-serif;background:#0f1115;color:#e7eaf0;display:flex;justify-content:center;padding-top:10vh}
.card{background:#171a21;border:1px solid #2a2f3a;border-radius:12px;padding:24px;width:min(420px,90vw)}
a.btn{display:block;margin:8px 0;padding:12px;border-radius:8px;background:#4c7dff;color:#fff;text-decoration:none;text-align:center}</style></head>
<body><main class="card"><h1>Multiple sessions share this port</h1>`)
	for _, c := range candidates {
		fmt.Fprintf(&b, `<a class="btn" href="http://%s">%s</a>`, c.CanonicalHost, c.CanonicalHost)
	}
	b.WriteString(`</main></body></html>`)
	return b.String()
}
