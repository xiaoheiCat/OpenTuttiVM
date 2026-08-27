package room

import "testing"

func TestRoomPasswordHashRoundTrip(t *testing.T) {
	h, err := HashRoomPassword("123456")
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyRoomPassword("123456", h) {
		t.Fatal("correct password rejected")
	}
	if VerifyRoomPassword("654321", h) {
		t.Fatal("wrong password accepted")
	}
}
