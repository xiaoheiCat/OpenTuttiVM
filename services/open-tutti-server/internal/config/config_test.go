package config

import (
	"os"
	"strings"
	"testing"
)

func TestLoadHTTPRequiresNonWildcardLoopback(t *testing.T) {
	cases := []struct {
		name string
		addr string
		ok   bool
	}{
		{"ipv4 loopback", "127.0.0.1:8080", true},
		{"localhost", "localhost:8080", true},
		{"ipv6 loopback", "[::1]:8080", true},
		{"empty host wildcard", ":8080", false},
		{"ipv4 wildcard", "0.0.0.0:8080", false},
		{"ipv6 wildcard", "[::]:8080", false},
		{"bare ipv6 wildcard", "::", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OPEN_TUTTI_SECRET", "test-secret")
			t.Setenv("OPEN_TUTTI_LISTEN_ADDR", tc.addr)
			t.Setenv("OPEN_TUTTI_PUBLIC_URL", "http://localhost:8080")
			_, err := Load("")
			if (err == nil) != tc.ok {
				t.Fatalf("Load(%q) error = %v, want success=%v", tc.addr, err, tc.ok)
			}
		})
	}
}

func TestLoadHTTPSAllowsWildcard(t *testing.T) {
	t.Setenv("OPEN_TUTTI_SECRET", "test-secret")
	t.Setenv("OPEN_TUTTI_LISTEN_ADDR", "[::]:8080")
	t.Setenv("OPEN_TUTTI_PUBLIC_URL", "https://example.test")
	if _, err := Load(""); err != nil {
		t.Fatal(err)
	}
}

func TestLoadCASQuotaOverride(t *testing.T) {
	t.Setenv("OPEN_TUTTI_SECRET", "test-secret")
	t.Setenv("OPEN_TUTTI_LISTEN_ADDR", "127.0.0.1:8080")
	t.Setenv("OPEN_TUTTI_PUBLIC_URL", "http://localhost:8080")
	t.Setenv("OPEN_TUTTI_CAS_ROOM_QUOTA_BYTES", "1234")
	cfg, err := Load("")
	if err != nil || cfg.CASRoomQuotaBytes != 1234 {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
	if strings.TrimSpace(os.Getenv("OPEN_TUTTI_SECRET")) == "" {
		t.Fatal("test environment was not set")
	}
}
