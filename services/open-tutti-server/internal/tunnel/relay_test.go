package tunnel

import (
	"errors"
	"testing"
)

func TestRelayStreamAdmissionPerDeviceAndTotal(t *testing.T) {
	r := NewRelayWithLimits(nil, nil, 2, 3)
	a, err := r.admitStream("room", "device")
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.admitStream("room", "device")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.admitStream("room", "device"); !errors.Is(err, ErrStreamLimit) {
		t.Fatalf("third stream error = %v", err)
	}
	c, err := r.admitStream("room", "other-device")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.admitStream("other-room", "other-device"); !errors.Is(err, ErrStreamLimit) {
		t.Fatalf("total-limit error = %v", err)
	}
	a.Release()
	d, err := r.admitStream("room", "device")
	if err != nil {
		t.Fatalf("released stream was not reusable: %v", err)
	}
	d.Release()
	b.Release()
	c.Release()
	if r.total != 0 || len(r.streams) != 0 {
		t.Fatalf("admission leaked: total=%d streams=%v", r.total, r.streams)
	}
}

func TestRelayStreamAdmissionReleaseIsIdempotent(t *testing.T) {
	r := NewRelayWithLimits(nil, nil, 1, 1)
	a, err := r.admitStream("room", "device")
	if err != nil {
		t.Fatal(err)
	}
	a.Release()
	a.Release()
	if _, err := r.admitStream("room", "device"); err != nil {
		t.Fatalf("stream not reusable after repeated close: %v", err)
	}
}

func TestRelayPendingHandshakeAdmissionReleaseKeepsOtherAdmission(t *testing.T) {
	r := NewRelayWithPendingLimits(nil, nil, 1, 1, 2, 2, 2)
	a, err := r.admitHandshake("room", "device", nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.admitHandshake("room", "device", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.admitHandshake("room", "device", nil); !errors.Is(err, ErrStreamLimit) {
		t.Fatalf("third handshake error = %v", err)
	}
	a.Release()
	if _, ok := r.pending[b]; !ok {
		t.Fatal("releasing one admission removed the other admission")
	}
	if r.pendingTotal != 1 || r.pendingRooms["room"] != 1 || r.pendingDevices[streamOwner{roomID: "room", deviceID: "device"}] != 1 {
		t.Fatalf("counters after releasing one admission: total=%d rooms=%v devices=%v", r.pendingTotal, r.pendingRooms, r.pendingDevices)
	}
	b.Release()
	if r.pendingTotal != 0 || len(r.pendingRooms) != 0 || len(r.pendingDevices) != 0 || len(r.pending) != 0 {
		t.Fatalf("counters after releasing both admissions: total=%d rooms=%v devices=%v pending=%v", r.pendingTotal, r.pendingRooms, r.pendingDevices, r.pending)
	}
	c, err := r.admitHandshake("room", "device", nil)
	if err != nil {
		t.Fatalf("released handshake was not reusable: %v", err)
	}
	c.Release()
}

func TestRelayPendingHandshakeAdmissionReleaseIsIdempotent(t *testing.T) {
	r := NewRelayWithPendingLimits(nil, nil, 1, 1, 1, 1, 1)
	a, err := r.admitHandshake("room", "device", nil)
	if err != nil {
		t.Fatal(err)
	}
	a.Release()
	a.Release()
	if r.pendingTotal != 0 || len(r.pendingRooms) != 0 || len(r.pendingDevices) != 0 || len(r.pending) != 0 {
		t.Fatalf("repeated release changed counters: total=%d rooms=%v devices=%v pending=%v", r.pendingTotal, r.pendingRooms, r.pendingDevices, r.pending)
	}
}
