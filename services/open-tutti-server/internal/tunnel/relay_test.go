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
