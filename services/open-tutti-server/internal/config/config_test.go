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

func TestLoadComposeLocalModeAllowsDockerBridgeListener(t *testing.T) {
	t.Setenv("OPEN_TUTTI_SECRET", "test-secret")
	t.Setenv("OPEN_TUTTI_LISTEN_ADDR", "0.0.0.0:8080")
	t.Setenv("OPEN_TUTTI_PUBLIC_URL", "http://localhost:8080")
	t.Setenv("OPEN_TUTTI_COMPOSE_LOCAL_MODE", "1")
	if _, err := Load(""); err != nil {
		t.Fatal(err)
	}
}

func TestLoadComposeLocalModeDoesNotAllowArbitraryLocalURL(t *testing.T) {
	t.Setenv("OPEN_TUTTI_SECRET", "test-secret")
	t.Setenv("OPEN_TUTTI_LISTEN_ADDR", "0.0.0.0:8080")
	t.Setenv("OPEN_TUTTI_PUBLIC_URL", "http://localhost:9999")
	t.Setenv("OPEN_TUTTI_COMPOSE_LOCAL_MODE", "1")
	if _, err := Load(""); err == nil {
		t.Fatal("expected non-default localhost URL to be rejected")
	}
}

func TestLoadEnvFileCannotEnableComposeLocalMode(t *testing.T) {
	t.Setenv("OPEN_TUTTI_SECRET", "test-secret")
	t.Setenv("OPEN_TUTTI_LISTEN_ADDR", "0.0.0.0:8080")
	t.Setenv("OPEN_TUTTI_PUBLIC_URL", "http://localhost:8080")
	t.Setenv("OPEN_TUTTI_COMPOSE_LOCAL_MODE", "")
	envFile := t.TempDir() + "/.env"
	if err := os.WriteFile(envFile, []byte("OPEN_TUTTI_COMPOSE_LOCAL_MODE=1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(envFile); err == nil {
		t.Fatal("expected .env mode marker to be ignored")
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

func TestLoadActiveRoomLimitOverride(t *testing.T) {
	t.Setenv("OPEN_TUTTI_SECRET", "test-secret")
	t.Setenv("OPEN_TUTTI_LISTEN_ADDR", "127.0.0.1:8080")
	t.Setenv("OPEN_TUTTI_PUBLIC_URL", "http://localhost:8080")
	t.Setenv("OPEN_TUTTI_ACTIVE_ROOM_LIMIT", "7")
	cfg, err := Load("")
	if err != nil || cfg.ActiveRoomLimit != 7 {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
}
