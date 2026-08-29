package gateway

import (
	"crypto/x509"
	"strings"
	"testing"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

type fakeLookup struct {
	deviceRoutes map[int][]vmprotocol.SessionCandidate
	sessions     map[string]vmprotocol.SessionCandidate
}

func (f *fakeLookup) LookupDevice(deviceSlug string, port int) ([]vmprotocol.SessionCandidate, error) {
	return f.deviceRoutes[port], nil
}

func (f *fakeLookup) LookupSession(deviceSlug, sessionID string, port int) (vmprotocol.SessionCandidate, error) {
	cand, ok := f.sessions[sessionID]
	if !ok {
		return vmprotocol.SessionCandidate{}, vmprotocol.ErrAmbiguousRoute
	}
	return cand, nil
}

func TestSessionAddressAlwaysRoutes(t *testing.T) {
	lookup := &fakeLookup{
		sessions: map[string]vmprotocol.SessionCandidate{
			"sess-claude-a": {SessionID: "sess-claude-a", SessionLabel: "claude-a", CanonicalHost: "claude-a.anna.tutti:3000"},
		},
	}
	res, err := Resolve(lookup, "claude-a.anna.tutti:3000")
	if err != nil {
		t.Fatalf("session resolve: %v", err)
	}
	if res.SessionID != "sess-claude-a" || res.Port != 3000 {
		t.Fatalf("resolution %+v", res)
	}
}

func TestDeviceAddressRules(t *testing.T) {
	solo := vmprotocol.SessionCandidate{SessionID: "sess-a", SessionLabel: "a", CanonicalHost: "a.anna.tutti:3000"}
	both := []vmprotocol.SessionCandidate{
		{SessionID: "sess-a", SessionLabel: "a", CanonicalHost: "a.anna.tutti:3000"},
		{SessionID: "sess-b", SessionLabel: "b", CanonicalHost: "b.anna.tutti:3000"},
	}

	// Unique occupancy routes transparently.
	lookup := &fakeLookup{deviceRoutes: map[int][]vmprotocol.SessionCandidate{3000: {solo}}}
	res, err := Resolve(lookup, "anna.tutti:3000")
	if err != nil || res.SessionID != "sess-a" {
		t.Fatalf("unique route: %+v err=%v", res, err)
	}

	// Ambiguity carries candidates for the HTTP selector; raw-TCP callers
	// fail with the protocol's guidance error.
	lookup = &fakeLookup{deviceRoutes: map[int][]vmprotocol.SessionCandidate{3000: both}}
	res, err = Resolve(lookup, "anna.tutti:3000")
	if err == nil {
		t.Fatal("ambiguity must surface the error")
	}
	if len(res.Candidates) != 2 {
		t.Fatalf("candidates %+v", res.Candidates)
	}
	page := SessionSelectorPage("zh-CN,zh;q=0.9,en;q=0.8", "https", res.Candidates)
	if !strings.Contains(page, "https://a.anna.tutti:3000") || !strings.Contains(page, "https://b.anna.tutti:3000") {
		t.Fatalf("selector page missing canonical links: %s", page)
	}
	// Localized copy negotiated from Accept-Language, and participant-
	// controlled labels stay escaped.
	if !strings.Contains(page, "选择会话") || strings.Contains(page, "<script>") {
		t.Fatalf("selector page localization/escaping: %s", page)
	}
	if en := SessionSelectorPage("", "https", res.Candidates); !strings.Contains(en, "Choose a session") {
		t.Fatalf("english fallback: %s", en)
	}

	// No listener: unresolved.
	lookup = &fakeLookup{}
	if _, err := Resolve(lookup, "anna.tutti:9999"); err == nil {
		t.Fatal("unoccupied port must not resolve")
	}
}

func TestVIPAllocatorStableAndScoped(t *testing.T) {
	a := NewVIPAllocator()
	anna, _ := vmprotocol.ParseTuttiHost("annasmacbookpro.tutti")
	ip1 := a.Assign(anna)
	ip2 := a.Assign(anna)
	if !ip1.Equal(ip2) {
		t.Fatalf("unstable VIP %s %s", ip1, ip2)
	}
	if ip1[0] != 100 || ip1[1] != 96 {
		t.Fatalf("VIP outside reserved block: %s", ip1)
	}
	leo, _ := vmprotocol.ParseTuttiHost("leospc.tutti")
	ipLeo := a.Assign(leo)
	if ipLeo.Equal(ip1) {
		t.Fatalf("devices share a VIP: %s", ipLeo)
	}
	a.ReleaseAll()
	if _, ok := a.Lookup(anna); ok {
		t.Fatal("release-all must clear assignments")
	}
}

func TestLocalCASignsTuttiHostsOnlyInsideTrust(t *testing.T) {
	ca, err := NewLocalCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, err := ca.LeafFor("claude-a.anna.tutti")
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: ca.VerifyInTuttiRuntime(), DNSName: "claude-a.anna.tutti"}); err != nil {
		t.Fatalf("room CA must sign .tutti leaves: %v", err)
	}
	// System-style verification (no room root) must fail — the host OS
	// never trusts .tutti by design.
	if _, err := leaf.Verify(x509.VerifyOptions{DNSName: "claude-a.anna.tutti"}); err == nil {
		t.Fatal(".tutti leaf must not verify without the room CA")
	}
	// Cached leaf reuse.
	cached, err := ca.LeafFor("claude-a.anna.tutti")
	if err != nil || string(cached.Certificate[0]) != string(cert.Certificate[0]) {
		t.Fatal("leaf cache miss")
	}
}
