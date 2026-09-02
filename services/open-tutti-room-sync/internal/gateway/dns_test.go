package gateway

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// buildQuery encodes a minimal A-query packet for name.
func buildQuery(t *testing.T, name string) []byte {
	t.Helper()
	q := []byte{0x12, 0x34, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0}
	for _, label := range splitLabels(name) {
		q = append(q, byte(len(label)))
		q = append(q, label...)
	}
	q = append(q, 0)
	q = binary.BigEndian.AppendUint16(q, 1) // A
	q = binary.BigEndian.AppendUint16(q, 1) // IN
	return q
}

func splitLabels(name string) []string {
	var out []string
	start := 0
	for i := 0; i < len(name); i++ {
		if name[i] == '.' {
			out = append(out, name[start:i])
			start = i + 1
		}
	}
	return append(out, name[start:])
}

// TestDNSAnswerCarriesANonzeroANCOUNT pins the wire shape: the A record
// must be declared in ANCOUNT at bytes 6-7 (a stray NSCOUNT write there
// once zeroed it, making the answer invisible to resolvers).
func TestDNSAnswerCarriesANonzeroANCOUNT(t *testing.T) {
	vips := NewVIPAllocator()
	s := NewDNSServer(vips)
	host := vmprotocol.TuttiHost{Device: "self", Session: "sess-claude"}
	want := vips.Assign(host)
	s.SetHostResolver(func(name string) (net.IP, bool) {
		if name != host.String() {
			return nil, false
		}
		return vips.Lookup(host)
	})
	resp := s.answer(buildQuery(t, "sess-claude.self.tutti"))
	if resp == nil {
		t.Fatal("no answer for .tutti A query")
	}
	if got := binary.BigEndian.Uint16(resp[6:8]); got != 1 {
		t.Fatalf("ANCOUNT = %d, want 1", got)
	}
	if got := binary.BigEndian.Uint16(resp[8:10]); got != 0 {
		t.Fatalf("NSCOUNT = %d, want 0", got)
	}
	// The answer's RDATA (last 4 bytes) is the synthetic VIP.
	if got := net.IP(resp[len(resp)-4:]); !got.Equal(want) {
		t.Fatalf("answer IP = %v, want %v", got, want)
	}
}

func TestDNSNonTuttiGetsNODATA(t *testing.T) {
	s := NewDNSServer(NewVIPAllocator())
	resp := s.answer(buildQuery(t, "example.com"))
	if resp == nil {
		t.Fatal("NODATA must still be a well-formed empty response")
	}
	if got := binary.BigEndian.Uint16(resp[6:8]); got != 0 {
		t.Fatalf("ANCOUNT = %d, want 0 for foreign names", got)
	}
}

func TestDNSOverUDPAnswersQueries(t *testing.T) {
	vips := NewVIPAllocator()
	s := NewDNSServer(vips)
	host := vmprotocol.TuttiHost{Device: "self", Session: "db"}
	vips.Assign(host)
	s.SetHostResolver(func(name string) (net.IP, bool) {
		if name != host.String() {
			return nil, false
		}
		return vips.Lookup(host)
	})
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	client, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	go func() {
		buf := make([]byte, 1500)
		n, from, err := pc.ReadFrom(buf)
		if err == nil {
			s.respond(pc, from, buf[:n])
		}
	}()
	q := buildQuery(t, "db.self.tutti")
	if _, err := client.WriteTo(q, pc.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1500)
	if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	n, _, err := client.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	resp := buf[:n]
	if got := binary.BigEndian.Uint16(resp[6:8]); got != 1 {
		t.Fatalf("ANCOUNT = %d over UDP", got)
	}
	// Response ends with the 16-byte A record: ptr(2) type(2) class(2)
	// ttl(4) rdlength(2) rdata(4).
	if got := binary.BigEndian.Uint16(resp[n-16 : n-14]); got != 0xc00c {
		t.Fatalf("answer name pointer = %x", got)
	}
	if got := binary.BigEndian.Uint16(resp[n-14 : n-12]); got != 1 {
		t.Fatalf("answer TYPE = %d, want A(1)", got)
	}
	want, ok := vips.Lookup(vmprotocol.TuttiHost{Device: "self", Session: "db"})
	if !ok {
		t.Fatal("responder did not assign a VIP for the queried host")
	}
	if got := net.IP(resp[n-4:]); !got.Equal(want) {
		t.Fatalf("answer IP = %v, want %v", got, want)
	}
}

func TestDNSRouteRemovalReturnsNODATAWithoutReallocating(t *testing.T) {
	vips := NewVIPAllocator()
	proxy := &Proxy{vips: vips, listeners: map[string]*routeBinding{}}
	host := vmprotocol.TuttiHost{Device: "self", Session: "gone"}
	ip := vips.Assign(host)
	proxy.listeners["listener"] = &routeBinding{target: &routeTarget{host: host.String()}}
	s := NewDNSServer(vips)
	s.SetHostResolver(proxy.ResolveHost)
	if got, ok := s.resolve(host.String()); !ok || !got.Equal(ip) {
		t.Fatalf("live host resolution = %v, %v; want %v, true", got, ok, ip)
	}
	proxy.mu.Lock()
	delete(proxy.listeners, "listener")
	vips.Release(host)
	proxy.mu.Unlock()
	if got, ok := s.resolve(host.String()); ok || got != nil {
		t.Fatalf("dead host resolution = %v, %v; want nil, false", got, ok)
	}
	if _, ok := vips.Lookup(host); ok {
		t.Fatal("dead host acquired a VIP after route removal")
	}
	resp := s.answer(buildQuery(t, host.String()))
	if got := binary.BigEndian.Uint16(resp[6:8]); got != 0 {
		t.Fatalf("dead host ANCOUNT = %d, want NODATA", got)
	}
}

func TestDNSSharedModeReadsLiveBindingOnly(t *testing.T) {
	vips := NewVIPAllocator()
	vips.mode.Store(int32(modeShared))
	proxy := &Proxy{vips: vips, listeners: map[string]*routeBinding{}}
	host := vmprotocol.TuttiHost{Device: "self", Session: "shared"}
	proxy.listeners["listener"] = &routeBinding{target: &routeTarget{host: host.String()}}
	s := NewDNSServer(vips)
	s.SetHostResolver(proxy.ResolveHost)
	resp := s.answer(buildQuery(t, host.String()))
	if got := binary.BigEndian.Uint16(resp[6:8]); got != 1 {
		t.Fatalf("shared live host ANCOUNT = %d, want 1", got)
	}
	proxy.mu.Lock()
	delete(proxy.listeners, "listener")
	proxy.mu.Unlock()
	resp = s.answer(buildQuery(t, host.String()))
	if got := binary.BigEndian.Uint16(resp[6:8]); got != 0 {
		t.Fatalf("shared dead host ANCOUNT = %d, want NODATA", got)
	}
}
