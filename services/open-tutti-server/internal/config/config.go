// Package config loads the open-tutti-server configuration with the fixed
// precedence: real environment variables override .env, .env overrides
// program defaults.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Config is the resolved server configuration.
type Config struct {
	ListenAddr   string
	PublicURL    string
	DataDir      string
	DatabasePath string
	ObjectsDir   string
	LogLevel     string
	Secret       string
	// ServerInviteCode is optional; when set, room creation requires it.
	// It is validated per request against the current value — there is no
	// long-lived server-side grant list.
	ServerInviteCode string
	// OwnerGracePeriod is how long an unexpectedly disconnected owner keeps
	// the room before ownership auto-transfers (or the room dissolves when
	// nobody is online).
	OwnerGracePeriod        time.Duration
	BorrowerDisconnectGrace time.Duration
	// JoinTicketTTL bounds the one-time share join tickets.
	JoinTicketTTL time.Duration
	// SnapshotIntervalOps triggers a checkpoint after this many operations.
	SnapshotIntervalOps           int
	CASRoomQuotaBytes             int64
	CASPendingQuotaBytes          int64
	CASPendingTTL                 time.Duration
	WorkspaceMaxEntries           int
	WorkspaceMaxPathBytes         int64
	WorkspaceMaxLivePathBytes     int64
	WorkspaceMaxIdentities        int
	WorkspaceMaxIdentityBytes     int64
	WorkspaceMaxOperationIDBytes  int
	WorkspaceMaxAgentSessionBytes int
	ActiveRoomLimit               int
}

// Load resolves configuration from env, then envFile (.env), then defaults.
// envFile may be empty; a missing file is not an error.
func Load(envFile string) (Config, error) {
	fileValues, err := parseEnvFile(envFile)
	if err != nil {
		return Config{}, err
	}
	get := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		if v := fileValues[key]; v != "" {
			return v
		}
		return def
	}
	// This marker is deliberately read only from the process environment. It is
	// injected by the checked-in Compose service and cannot be enabled through
	// .env, which is user-controlled configuration.
	composeLocalMode := os.Getenv("OPEN_TUTTI_COMPOSE_LOCAL_MODE") == "1"

	cfg := Config{
		ListenAddr:              get("OPEN_TUTTI_LISTEN_ADDR", "127.0.0.1:8080"),
		PublicURL:               get("OPEN_TUTTI_PUBLIC_URL", "http://localhost:8080"),
		DataDir:                 get("OPEN_TUTTI_DATA_DIR", defaultDataDir()),
		LogLevel:                get("OPEN_TUTTI_LOG_LEVEL", "info"),
		Secret:                  get("OPEN_TUTTI_SECRET", ""),
		ServerInviteCode:        get("OPEN_TUTTI_SERVER_INVITE_CODE", ""),
		OwnerGracePeriod:        secondsOrDefault(get("OPEN_TUTTI_OWNER_GRACE_SECONDS", ""), 5*time.Minute),
		BorrowerDisconnectGrace: secondsOrDefault(get("OPEN_TUTTI_BORROWER_DISCONNECT_GRACE_SECONDS", ""), 5*time.Minute),
		JoinTicketTTL:           secondsOrDefault(get("OPEN_TUTTI_JOIN_TICKET_TTL_SECONDS", ""), 60*time.Second),
		// An operation count, not a duration.
		SnapshotIntervalOps:           intOrDefault(get("OPEN_TUTTI_SNAPSHOT_INTERVAL_OPS", ""), 512),
		CASRoomQuotaBytes:             int64OrDefault(get("OPEN_TUTTI_CAS_ROOM_QUOTA_BYTES", ""), 1<<30),
		CASPendingQuotaBytes:          int64OrDefault(get("OPEN_TUTTI_CAS_PENDING_QUOTA_BYTES", ""), 64<<20),
		CASPendingTTL:                 secondsOrDefault(get("OPEN_TUTTI_CAS_PENDING_TTL_SECONDS", ""), 15*time.Minute),
		WorkspaceMaxEntries:           intOrDefault(get("OPEN_TUTTI_WORKSPACE_MAX_ENTRIES", ""), 100000),
		WorkspaceMaxPathBytes:         int64OrDefault(get("OPEN_TUTTI_WORKSPACE_MAX_PATH_BYTES", ""), 16<<20),
		WorkspaceMaxLivePathBytes:     int64OrDefault(get("OPEN_TUTTI_WORKSPACE_MAX_LIVE_PATH_BYTES", ""), 64<<20),
		WorkspaceMaxIdentities:        intOrDefault(get("OPEN_TUTTI_WORKSPACE_MAX_IDENTITIES", ""), 200000),
		WorkspaceMaxIdentityBytes:     int64OrDefault(get("OPEN_TUTTI_WORKSPACE_MAX_IDENTITY_BYTES", ""), 32<<20),
		WorkspaceMaxOperationIDBytes:  intOrDefault(get("OPEN_TUTTI_WORKSPACE_MAX_OPERATION_ID_BYTES", ""), 1024),
		WorkspaceMaxAgentSessionBytes: intOrDefault(get("OPEN_TUTTI_WORKSPACE_MAX_AGENT_SESSION_BYTES", ""), 1024),
		ActiveRoomLimit:               intOrDefault(get("OPEN_TUTTI_ACTIVE_ROOM_LIMIT", ""), 100),
	}

	cfg.DatabasePath = get("OPEN_TUTTI_DATABASE_PATH", filepath.Join(cfg.DataDir, "open-tutti.db"))
	cfg.ObjectsDir = get("OPEN_TUTTI_OBJECTS_DIR", filepath.Join(cfg.DataDir, "objects"))

	if cfg.Secret == "" {
		return Config{}, errors.New("OPEN_TUTTI_SECRET must be set (generate one, e.g. `openssl rand -hex 32`)")
	}
	u, err := url.Parse(cfg.PublicURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return Config{}, errors.New("OPEN_TUTTI_PUBLIC_URL must be an absolute URL")
	}
	if u.Scheme == "http" {
		if !isLoopbackListenAddr(cfg.ListenAddr) && !isComposeLocalMode(composeLocalMode, cfg) {
			return Config{}, errors.New("plain HTTP public URL requires a loopback listen address; use a TLS reverse proxy for remote deployment")
		}
	}
	if cfg.OwnerGracePeriod <= 0 || cfg.BorrowerDisconnectGrace <= 0 || cfg.JoinTicketTTL <= 0 || cfg.SnapshotIntervalOps <= 0 || cfg.CASRoomQuotaBytes <= 0 || cfg.CASPendingQuotaBytes <= 0 || cfg.CASPendingTTL <= 0 || cfg.WorkspaceMaxEntries <= 0 || cfg.WorkspaceMaxPathBytes <= 0 || cfg.WorkspaceMaxLivePathBytes <= 0 || cfg.WorkspaceMaxIdentities <= 0 || cfg.WorkspaceMaxIdentityBytes <= 0 || cfg.WorkspaceMaxOperationIDBytes <= 0 || cfg.WorkspaceMaxAgentSessionBytes <= 0 || cfg.ActiveRoomLimit <= 0 {
		return Config{}, errors.New("grace period, ticket TTL, snapshot interval, CAS quotas, workspace limits, and active room limit must be positive")
	}
	return cfg, nil
}

func isComposeLocalMode(enabled bool, cfg Config) bool {
	return enabled && cfg.ListenAddr == "0.0.0.0:8080" && cfg.PublicURL == "http://localhost:8080"
}

func isLoopbackListenAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

// intOrDefault parses a plain positive integer override (defaults on
// empty or invalid input).
func intOrDefault(v string, def int) int {
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func int64OrDefault(v string, def int64) int64 {
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// parseEnvFile reads KEY=VALUE lines; comments (#) and blank lines are
// ignored; surrounding quotes are stripped.
func parseEnvFile(path string) (map[string]string, error) {
	out := map[string]string{}
	if path == "" {
		return out, nil
	}
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, nil
		}
		return nil, fmt.Errorf("open env file: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if k != "" {
			out[k] = v
		}
	}
	return out, sc.Err()
}

func secondsOrDefault(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return def
	}
	return time.Duration(v) * time.Second
}

// defaultDataDir keeps the container/server path on POSIX and derives a
// per-user location on Windows: a POSIX-rooted default under the current
// drive root is unwritable for a normal non-elevated Windows user running
// the server from source.
func defaultDataDir() string {
	if runtime.GOOS == "windows" {
		if base, err := os.UserConfigDir(); err == nil {
			return filepath.Join(base, "open-tutti")
		}
	}
	return "/var/lib/open-tutti"
}
