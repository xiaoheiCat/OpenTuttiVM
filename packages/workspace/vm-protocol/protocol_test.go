package vmprotocol

import (
	"strings"
	"testing"
)

func TestEnvelopeJSONRoundTrip(t *testing.T) {
	env := Envelope{
		RoomID:         "room_1",
		OperationID:    "op_1",
		AuthorDeviceID: "dev_a",
		AgentSessionID: "sess_claude",
		BaseSeq:        100,
		ServerSeq:      101,
		TimestampMS:    1770000000000,
		Operation: FileOperation{
			ID:   "op_1",
			Path: "src/app.ts",
			Kind: OpTextPatch,
			Patch: &TextPatch{
				BaseHash: "sha256:aaaa",
				Splices: []Splice{
					{Offset: 20, DeleteLen: 2, Insert: "const port = 8080"},
				},
			},
		},
	}
	data, err := env.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeEnvelope(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Operation.Kind != OpTextPatch || got.Operation.Path != "src/app.ts" {
		t.Fatalf("operation lost: %+v", got.Operation)
	}
	if got.Operation.Patch.Splices[0].Insert != "const port = 8080" {
		t.Fatalf("splice lost: %+v", got.Operation.Patch)
	}
	if got.ServerSeq != 101 || got.BaseSeq != 100 {
		t.Fatalf("sequence lost: %+v", got)
	}
}

func TestParseTuttiHost(t *testing.T) {
	h, err := ParseTuttiHost("claude-a.AnnasMacBookPro.tutti")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if h.Session != "claude-a" || h.Device != "annasmacbookpro" || !h.IsSessionLevel() {
		t.Fatalf("unexpected host: %+v", h)
	}
	if want := "claude-a.annasmacbookpro.tutti"; h.String() != want {
		t.Fatalf("canonical = %q want %q", h.String(), want)
	}

	d, err := ParseTuttiHost("leospc.tutti")
	if err != nil {
		t.Fatalf("parse device: %v", err)
	}
	if d.IsSessionLevel() || d.Device != "leospc" {
		t.Fatalf("unexpected device host: %+v", d)
	}

	for _, bad := range []string{"example.com", "a.b.c.tutti", "-bad.tutti", "bad-.tutti", "tutti"} {
		if _, err := ParseTuttiHost(bad); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
}

func TestSlugifyHostname(t *testing.T) {
	if got := SlugifyHostname("Anna's MacBook Pro"); got != "anna-s-macbook-pro" {
		t.Fatalf("slug = %q", got)
	}
	if got := SlugifyHostname("😀"); got != "device" {
		t.Fatalf("fallback slug = %q", got)
	}
}

func TestResolveDeviceRouteRules(t *testing.T) {
	// Zero occupancy stays unresolved without error.
	res, err := ResolveDeviceRoute(3000, nil)
	if err != nil || res.Resolved {
		t.Fatalf("empty resolution: %+v err=%v", res, err)
	}

	// Unique occupancy routes transparently.
	solo := []SessionCandidate{{SessionID: "s1", SessionLabel: "claude-a", CanonicalHost: "claude-a.anna.tutti:3000"}}
	res, err = ResolveDeviceRoute(3000, solo)
	if err != nil || !res.Resolved || res.SessionID != "s1" {
		t.Fatalf("unique resolution: %+v err=%v", res, err)
	}

	// Ambiguity is reported; candidates ordered by canonical host so the H5
	// selector is deterministic.
	both := []SessionCandidate{
		{SessionID: "s2", SessionLabel: "codex-b", CanonicalHost: "codex-b.anna.tutti:3000"},
		{SessionID: "s1", SessionLabel: "claude-a", CanonicalHost: "claude-a.anna.tutti:3000"},
	}
	res, err = ResolveDeviceRoute(3000, both)
	if err == nil {
		t.Fatal("expected ambiguity error for raw TCP")
	}
	if !strings.Contains(err.Error(), "full session hostname") {
		t.Fatalf("error must point at session hostname, got: %v", err)
	}
	res, _ = ResolveDeviceRoute(3000, both)
	if res.Resolved || len(res.Candidates) != 2 || res.Candidates[0].CanonicalHost != "claude-a.anna.tutti:3000" {
		t.Fatalf("ambiguous candidates: %+v", res.Candidates)
	}
}

func TestTunnelHeaderRoundTrip(t *testing.T) {
	h := TunnelHeader{
		Action:   TunnelConnect,
		RoomID:   "room_1",
		DeviceID: "dev_bob",
		Route:    RouteKey{RoomID: "room_1", DeviceID: "dev_anna", SessionID: "sess_1", Port: 3000},
	}
	data, err := h.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeTunnelHeader(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Route != h.Route || got.Action != TunnelConnect {
		t.Fatalf("header lost: %+v", got)
	}
}
