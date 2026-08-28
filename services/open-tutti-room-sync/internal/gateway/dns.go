// .tutti name resolution: a minimal UDP DNS responder mapping every
// virtual hostname onto its synthetic VIP so containers in the room
// network can dial names instead of memorizing 100.96.x.y addresses.
package gateway

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// DNSServer answers A queries for *.tutti with the allocator's synthetic
// address. Only the room's own runtimes point their resolvers here; the
// host OS resolver is untouched.
type DNSServer struct {
	vips *VIPAllocator
}

// NewDNSServer wires the responder onto the process-wide VIP allocator so
// DNS answers and listener bindings always agree.
func NewDNSServer(vips *VIPAllocator) *DNSServer {
	return &DNSServer{vips: vips}
}

// ListenAndServe binds the UDP socket and answers queries until it closes.
func (s *DNSServer) ListenAndServe(addr string) error {
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return fmt.Errorf("tutti dns listen %s: %w", addr, err)
	}
	defer pc.Close()
	buf := make([]byte, 1500)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			return err
		}
		msg := make([]byte, n)
		copy(msg, buf[:n])
		go s.respond(pc, from, msg)
	}
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
		return buildHeader(msg, 0)
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
	return append(buildHeader(msg, 1), rr...)
}

func buildHeader(msg []byte, answers int) []byte {
	out := make([]byte, 12, 12+len(msg)+16)
	copy(out, msg[:12])
	out[2] = 0x81 // response, recursion not desired
	out[3] = 0x80 // no error
	binary.BigEndian.PutUint16(out[6:], uint16(answers))
	binary.BigEndian.PutUint16(out[7:], 0)
	return append(out, msg[12:]...)
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
