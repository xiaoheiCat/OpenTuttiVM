// .tutti name resolution: a minimal UDP DNS responder mapping every
// virtual hostname onto its synthetic VIP so containers in the room
// network can dial names instead of memorizing 100.96.x.y addresses.
package gateway

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

const (
	dnsWorkerCount = 8
	dnsQueueSize   = 128
)

type dnsPacket struct {
	from net.Addr
	msg  []byte
}

// DNSServer answers A queries for *.tutti with the allocator's synthetic
// address. Only the room's own runtimes point their resolvers here; the
// host OS resolver is untouched.
type DNSServer struct {
	vips *VIPAllocator
	// known (optional) gates Assign to REGISTERED routes: without it
	// every syntactically valid .tutti name permanently consumed a VIP
	// allocation (the listener binds :1053 for every room session), so
	// a flood of unique names grew the heap without bound and, in VIP
	// mode, exhausted the ~40k address combinations — later LEGITIMATE
	// routes then collided and failed to bind.
	known func(host string) bool
}

// NewDNSServer wires the responder onto the process-wide VIP allocator so
// DNS answers and listener bindings always agree.
func NewDNSServer(vips *VIPAllocator) *DNSServer {
	return &DNSServer{vips: vips}
}

// SetKnownHosts installs the registered-route gate (the proxy's live
// bindings).
func (s *DNSServer) SetKnownHosts(known func(host string) bool) {
	s.known = known
}

// ListenAndServe binds the UDP socket and answers queries until it closes.
// Bind opens the UDP socket so callers FAIL STARTUP on an occupied or
// forbidden address instead of logging from a goroutine and reporting
// readiness with a broken resolver.
func (s *DNSServer) Bind(addr string) (net.PacketConn, error) {
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("tutti dns listen %s: %w", addr, err)
	}
	return pc, nil
}

func (s *DNSServer) Serve(pc net.PacketConn) error {
	defer pc.Close()
	jobs := make(chan dnsPacket, dnsQueueSize)
	var workers sync.WaitGroup
	workers.Add(dnsWorkerCount)
	for range dnsWorkerCount {
		go func() {
			defer workers.Done()
			for packet := range jobs {
				s.respond(pc, packet.from, packet.msg)
			}
		}()
	}
	defer func() {
		close(jobs)
		workers.Wait()
	}()
	buf := make([]byte, 1500)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			return err
		}
		msg := make([]byte, n)
		copy(msg, buf[:n])
		select {
		case jobs <- dnsPacket{from: from, msg: msg}:
		default:
			// UDP has no backpressure contract; overload is dropped so packet
			// floods cannot create unbounded goroutines or retained buffers.
		}
	}
}

// ListenAndServe binds and serves (kept for callers that manage their
// own failure handling).
func (s *DNSServer) ListenAndServe(addr string) error {
	pc, err := s.Bind(addr)
	if err != nil {
		return err
	}
	return s.Serve(pc)
}

func (s *DNSServer) respond(pc net.PacketConn, from net.Addr, msg []byte) {
	answer := s.answer(msg)
	if answer != nil {
		pc.WriteTo(answer, from) // best effort
	}
}

// answer builds the wire response for one query, or nil for garbage.
func (s *DNSServer) answer(msg []byte) []byte {
	if len(msg) < 12 {
		return nil
	}
	qEnd, name, ok := parseQuestion(msg)
	if !ok {
		return nil
	}
	// Only A queries for *.tutti resolve; everything else gets an empty
	// NODATA response so misconfigured resolvers fail fast.
	qType := binary.BigEndian.Uint16(msg[qEnd-4:])
	host, err := vmprotocol.ParseTuttiHost(strings.ToLower(name))
	if err != nil || qType != 1 {
		return buildHeader(msg, qEnd, 0)
	}
	if s.known != nil && !s.known(host.String()) {
		// Unregistered name: NODATA, no allocation retained.
		return buildHeader(msg, qEnd, 0)
	}
	ip := s.vips.Assign(host)
	rr := make([]byte, 0, 16)
	var ipb [4]byte
	copy(ipb[:], ip.To4())
	rr = append(rr, 0xc0, 0x0c) // name pointer
	rr = binary.BigEndian.AppendUint16(rr, 1)
	rr = binary.BigEndian.AppendUint16(rr, 1)
	rr = binary.BigEndian.AppendUint32(rr, 5)
	rr = binary.BigEndian.AppendUint16(rr, 4)
	rr = append(rr, ipb[:]...)
	return append(buildHeader(msg, qEnd, 1), rr...)
}

// buildHeader copies ONLY the header and question (through qEnd): the
// request body may carry an EDNS OPT additional record, and copying it
// while declaring ARCOUNT=0 leaves a stray record where parsers expect
// the answer section, pushing the appended A record past the declared
// sections so EDNS-capable resolvers see no usable address.
func buildHeader(msg []byte, qEnd int, answers int) []byte {
	out := make([]byte, qEnd, qEnd+16)
	copy(out, msg[:qEnd])
	out[2] = 0x81 // response, recursion not desired
	out[3] = 0x80 // no error
	// ANCOUNT/NSCOUNT/ARCOUNT live at 6-7/8-9/10-11; write each pair at
	// its own offset so nothing overlaps the answer count.
	binary.BigEndian.PutUint16(out[6:], uint16(answers))
	binary.BigEndian.PutUint16(out[8:], 0)
	binary.BigEndian.PutUint16(out[10:], 0)
	return out
}

// parseQuestion walks the question section and returns the offset just
// past it plus the decoded QNAME (dotted, no trailing dot).
func parseQuestion(msg []byte) (int, string, bool) {
	q := 12
	var labels []string
	for {
		if q >= len(msg) {
			return 0, "", false
		}
		n := int(msg[q])
		if n == 0 {
			q++
			break
		}
		if n&0xc0 != 0 || q+1+n > len(msg) {
			return 0, "", false
		}
		labels = append(labels, string(msg[q+1:q+1+n]))
		q += 1 + n
	}
	if q+4 > len(msg) {
		return 0, "", false
	}
	return q + 4, strings.Join(labels, "."), true
}
