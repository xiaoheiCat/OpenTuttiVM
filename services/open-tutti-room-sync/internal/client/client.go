// Package client is room-sync's connection to open-tutti-server: the
// business WebSocket with reconnect and sequence-gap resync, ticket
// redemption, and CAS chunk fetch/upload helpers.
package client

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	borrowagent "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/borrow"
	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
	vmsync "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync"
)

// Server identifies one self-hosted server.
type Server struct {
	BaseURL string
}

// Client talks to one server for one device.
type Client struct {
	server   Server
	http     *http.Client
	deviceID string

	mu     sync.Mutex
	token  string
	roomID string
}

// New creates a client. Every request carries a finite bound: several
// filesystem-critical paths deliberately use context.Background() (lazy
// CAS reads/uploads, route lookups), so a server that accepts the
// connection but never answers would otherwise hang a FUSE read or
// flush forever — a zero-timeout client has no such backstop.
func New(server Server, deviceID string) *Client {
	return &Client{server: server, http: &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
		},
	}, deviceID: deviceID}
}

// AdoptToken installs a session token minted by an earlier join (the
// room-sync container receives it via environment).
func (c *Client) AdoptToken(token string) error {
	parts := strings.Split(token, ".")
	if len(parts) != 4 {
		return errors.New("malformed session token")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token = token
	c.roomID = parts[0]
	return nil
}

// DeviceInfo enrolls the device on create/join calls.
type DeviceInfo struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	Hostname    string `json:"hostname"`
	PublicKey   string `json:"public_key"`
	// Proof is the base64 Ed25519 signature over the call's challenge
	// ("open-tutti-join:"+ticket when joining, CreateRoomProofMessage
	// when creating under an enrolled id). Required whenever the device
	// id is already enrolled on the server.
	Proof string `json:"proof,omitempty"`
}

// deviceProofDomain mirrors the server's proof domain: signatures cover
// Domain+challenge.
const deviceProofDomain = "open-tutti-join:"

// SignDeviceProof signs a server challenge with the device's Ed25519
// private key (PEM) and returns the base64 proof the server expects
// (join redemption passes the ticket as the challenge; room creation
// under an enrolled id passes "room-create:"+deviceID). The PEM block
// carries the raw seed (32), raw private key (64), or PKCS#8 form.
func SignDeviceProof(privateKeyPEM, challenge string) (string, error) {
	block, _ := pem.Decode([]byte(privateKeyPEM))
	if block == nil {
		return "", errors.New("malformed device private key")
	}
	var key ed25519.PrivateKey
	switch len(block.Bytes) {
	case ed25519.SeedSize:
		key = ed25519.NewKeyFromSeed(block.Bytes)
	case ed25519.PrivateKeySize:
		key = ed25519.PrivateKey(append([]byte(nil), block.Bytes...))
	default:
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return "", err
		}
		k, ok := parsed.(ed25519.PrivateKey)
		if !ok {
			return "", errors.New("device private key is not Ed25519")
		}
		key = k
	}
	sig := ed25519.Sign(key, []byte(deviceProofDomain+challenge))
	return base64.StdEncoding.EncodeToString(sig), nil
}

// CreateRoom creates a room on the server, submitting the server invite
// code with the request (the code is validated per create, never stored
// server-side).
func (c *Client) CreateRoom(ctx context.Context, inviteCode string, device DeviceInfo) (created struct {
	RoomID       string `json:"room_id"`
	ShareID      string `json:"share_id"`
	ShareURL     string `json:"share_url"`
	Password     string `json:"password"`
	SessionToken string `json:"session_token"`
}, err error) {
	body := map[string]any{"invite_code": inviteCode, "device": device}
	err = postJSON(ctx, c.http, c.server.BaseURL+"/api/rooms", body, &created)
	if err == nil {
		c.mu.Lock()
		c.token, c.roomID = created.SessionToken, created.RoomID
		c.mu.Unlock()
	}
	return created, err
}

// JoinTicket fetches a one-time join ticket from the share page flow.
func (c *Client) JoinTicket(ctx context.Context, shareID, password string) (ticket string, err error) {
	var res struct {
		Ticket string `json:"ticket"`
	}
	err = postJSON(ctx, c.http, c.server.BaseURL+"/api/share/"+shareID+"/join-ticket",
		map[string]string{"password": password}, &res)
	return res.Ticket, err
}

// Join redeems a ticket and returns the bootstrap snapshot plus ops.
func (c *Client) Join(ctx context.Context, roomID, ticket string, device DeviceInfo) (token string, snap vmprotocol.WorkspaceSnapshot, ops []vmprotocol.Envelope, err error) {
	var res struct {
		RoomID       string                       `json:"room_id"`
		SessionToken string                       `json:"session_token"`
		Snapshot     vmprotocol.WorkspaceSnapshot `json:"snapshot"`
		Ops          []vmprotocol.Envelope        `json:"ops"`
	}
	err = postJSON(ctx, c.http, c.server.BaseURL+"/api/rooms/"+roomID+"/join",
		map[string]any{"ticket": ticket, "device": device}, &res)
	if err == nil {
		c.mu.Lock()
		c.token, c.roomID = res.SessionToken, res.RoomID
		c.mu.Unlock()
	}
	return res.SessionToken, res.Snapshot, res.Ops, err
}

// Bootstrap re-fetches the checkpoint and replay window for resync.
// BootstrapResult carries the authoritative snapshot, the replay
// window, and the room's current owner (callers derive replica policy
// from ownership).
type BootstrapResult struct {
	Snapshot      vmprotocol.WorkspaceSnapshot
	Ops           []vmprotocol.Envelope
	OwnerDeviceID string
}

func (c *Client) Bootstrap(ctx context.Context) (BootstrapResult, error) {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	var res struct {
		Snapshot      vmprotocol.WorkspaceSnapshot `json:"snapshot"`
		Ops           []vmprotocol.Envelope        `json:"ops"`
		OwnerDeviceID string                       `json:"owner_device_id"`
	}
	err := getJSON(ctx, c.http, c.server.BaseURL+"/api/rooms/"+roomID+"/bootstrap", token, &res)
	return BootstrapResult{Snapshot: res.Snapshot, Ops: res.Ops, OwnerDeviceID: res.OwnerDeviceID}, err
}

// Session is one business WebSocket connection.
type Session struct {
	conn   *websocket.Conn
	client *Client

	// SessionLabel is the human-facing label of this device's session
	// (default "main"); the registry id is "sess-"+label, matching the
	// gateway convention. Set before announcing ports.
	SessionLabel string

	// OnEvent receives every server event (operations, presence, conflicts,
	// ports, room ending).
	OnEvent func(ev vmprotocol.Event)

	// OnGap is called when a sequence gap requires a bootstrap resync; the
	// loop pauses until it returns.
	OnGap func() error

	ctx    context.Context
	cancel context.CancelFunc

	// writeMu serializes socket writes: the coder/websocket Conn allows
	// exactly one concurrent writer, and the heartbeat pings from their
	// own goroutine would otherwise race operation submissions.
	writeMu sync.Mutex
	// lastActivity is the last received-frame time (UnixNano): a
	// half-open socket (cable pull, NAT drop) keeps Read blocked
	// forever, so the heartbeat enforces an idle deadline and aborts.
	lastActivity atomic.Int64
}

// Dial opens the room WebSocket with the current session token.
func (c *Client) Dial(ctx context.Context) (*Session, error) {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	if token == "" {
		return nil, fmt.Errorf("no session token; join or create a room first")
	}
	wsURL := strings.Replace(c.server.BaseURL, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	// Credentials ride the Authorization header, never the URL: proxies
	// and request loggers record request targets, and the membership
	// token grants full room access until rotated.
	wsURL += "/api/rooms/" + roomID + "/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}},
	})
	if err != nil {
		return nil, err
	}
	// Accepted text patches carry up to 8 MiB of inserted text; the
	// library's 32 KiB default would close the socket on every large
	// broadcast and force the whole room to resync.
	conn.SetReadLimit(int64(vmsync.MaxTextFile)*6 + 1<<20)
	sctx, cancel := context.WithCancel(ctx)
	s := &Session{conn: conn, client: c, ctx: sctx, cancel: cancel}
	s.lastActivity.Store(time.Now().UnixNano())
	return s, nil
}

// OpApplier applies one sequenced envelope to the replica. *vmsync.Replica
// satisfies it; the replica.Manager wrapper serializes Room FS access
// against server events.
type OpApplier interface {
	ApplyServerOp(env vmprotocol.Envelope) (bool, error)
}

// Run pumps events until the socket dies. It applies envelope events to
// the replica, resyncing via OnGap on sequence gaps. TopicOperation events
// reach OnEvent only after they were applied, so invalidation callbacks
// observe post-apply state.
func (s *Session) Run(replica OpApplier) error {
	// Application-level liveness: the server's MarkOnline (and through
	// it the owner grace period) only advances when the connection
	// proves itself, and a half-open socket would otherwise block the
	// documented transfer/dissolution path indefinitely.
	go s.heartbeat(25*time.Second, 90*time.Second)
	for {
		_, data, err := s.conn.Read(s.ctx)
		if err != nil {
			return err
		}
		s.lastActivity.Store(time.Now().UnixNano())
		var msg struct {
			Type  string           `json:"type"`
			Event vmprotocol.Event `json:"event"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		if msg.Type != "event" {
			continue
		}
		if msg.Event.Topic == vmprotocol.TopicOperation {
			var env vmprotocol.Envelope
			if err := json.Unmarshal(msg.Event.Payload, &env); err != nil {
				continue
			}
			if _, err := replica.ApplyServerOp(env); err != nil {
				// Sequence gaps AND local apply failures (transient
				// CAS fetch, materialization errors) both leave the
				// replica behind the authoritative stream; resync in
				// either case or the mount stays stale if no later
				// operation arrives.
				if s.OnGap != nil {
					if err := s.OnGap(); err != nil {
						return err
					}
				} else {
					return err
				}
			}
		}
		if s.OnEvent != nil {
			s.OnEvent(msg.Event)
		}
	}
}

// Submit sends one local operation.
// heartbeat sends application-level pings while the socket is quiet and
// enforces an idle deadline: a silent-but-dead socket (cable pull, NAT
// rebind) leaves Read blocked, the server's MarkOffline never runs, and
// the owner grace period would stall until platform TCP keepalives
// notice — or forever. Cancelling the context aborts Read; the caller's
// reconnect loop takes over.
func (s *Session) heartbeat(interval, idle time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	ping, _ := json.Marshal(struct {
		Type string `json:"type"`
	}{"ping"})
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			if time.Since(time.Unix(0, s.lastActivity.Load())) > idle {
				s.cancel()
				return
			}
			if err := s.write(ping); err != nil {
				return
			}
		}
	}
}

func (s *Session) Submit(env vmprotocol.Envelope) error {
	envBytes, err := env.Encode()
	if err != nil {
		return err
	}
	msg, err := json.Marshal(map[string]any{"type": "op", "envelope": json.RawMessage(envBytes)})
	if err != nil {
		return err
	}
	return s.write(msg)
}

// AnnouncePorts publishes a listening port for a local session.
func (s *Session) AnnouncePorts(p vmprotocol.PortsChangedPayload) error {
	msg, err := json.Marshal(map[string]any{"type": "ports", "ports": p})
	if err != nil {
		return err
	}
	return s.write(msg)
}

// ResolveBarrier lifts a conflict barrier this session was assigned to
// resolve (sent after the resolver's fix committed). The identity must
// be the registry id the bridge submits under ("sess-"+label) — the
// server's resolver match is exact.
func (s *Session) ResolveBarrier(path string) error {
	id := "sess-main"
	if s.SessionLabel != "" {
		id = "sess-" + s.SessionLabel
	}
	msg, err := json.Marshal(map[string]any{
		"type": "conflict_resolved", "path": path, "agent_session": id,
	})
	if err != nil {
		return err
	}
	return s.write(msg)
}

// writeTyped marshals one typed client message onto the socket.
func (s *Session) writeTyped(typ string, payload any) error {
	msg, err := json.Marshal(map[string]any{"type": typ, typ: payload})
	if err != nil {
		return err
	}
	return s.write(msg)
}

// write serializes socket writes: coder/websocket allows exactly one
// concurrent writer, and heartbeat pings run on their own goroutine.
// Each write runs on a BOUNDED child context: a peer that stops
// reading while holding the socket open would otherwise block this
// goroutine forever (the heartbeat's idle check lives in the SAME
// goroutine and can never fire), and every later submission queues
// behind writeMu — FUSE flushes hang with no reconnect.
func (s *Session) write(msg []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	err := s.conn.Write(ctx, websocket.MessageText, msg)
	if err != nil && ctx.Err() != nil {
		// The deadline fired: the socket is wedged — cancel the
		// session so the reconnect loop takes over instead of leaving
		// every caller to time out one by one.
		if s.cancel != nil {
			s.cancel()
		}
	}
	return err
}

// ReportPolicy tells the server this device's replica policy
// ("full"|"lazy"): automatic succession only promotes full replicas.
func (s *Session) ReportPolicy(policy string) error {
	return s.writeTyped("policy", map[string]string{"policy": policy})
}

// ReportTransferReady proves readiness from the candidate's authenticated
// room connection. The server accepts it only for the currently prepared
// generation and snapshot sequence.
func (c *Client) ReportTransferReady(ctx context.Context, generation string, snapshotSeq, appliedSeq uint64) error {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	return postJSON(ctx, c.http, c.server.BaseURL+"/api/rooms/"+roomID+"/transfer/ready", map[string]any{
		"generation": generation, "snapshot_seq": snapshotSeq, "applied_seq": appliedSeq,
	}, nil, authHeader(token))
}

// ShareAgent enables or disables borrowing for one local agent instance.
// The server stamps ownership from the authenticated connection.
func (s *Session) ShareAgent(p borrowagent.AgentSharedPayload) error {
	return s.writeTyped("agent_share", p)
}

// BorrowCommand sends one instruction to a shared agent; the server
// validates the lease generation and routes to the owner's device.
func (s *Session) BorrowCommand(p borrowagent.BorrowCommandPayload) error {
	return s.writeTyped("borrow_command", p)
}

func (s *Session) ReportBorrowCommandFailed(p borrowagent.CommandFailedPayload) error {
	return s.writeTyped("command_failed", p)
}

func (s *Session) ReportBorrowCommandFinished(p borrowagent.CommandFinishedPayload) error {
	return s.writeTyped("command_finished", p)
}

type TransferPreparation struct {
	Generation        string `json:"generation"`
	SnapshotSeq       uint64 `json:"snapshot_seq"`
	CandidateDeviceID string `json:"candidate_device_id"`
}

type TransferStatus struct {
	Pending           bool   `json:"pending"`
	Generation        string `json:"generation"`
	SnapshotSeq       uint64 `json:"snapshot_seq"`
	CandidateDeviceID string `json:"candidate_device_id"`
}

func (c *Client) TransferStatus(ctx context.Context) (TransferStatus, error) {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	var out TransferStatus
	err := getJSON(ctx, c.http, c.server.BaseURL+"/api/rooms/"+roomID+"/transfer/status", token, &out)
	return out, err
}

func (c *Client) PrepareTransfer(ctx context.Context, candidateDeviceID string) (TransferPreparation, error) {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	var out TransferPreparation
	err := postJSON(ctx, c.http, c.server.BaseURL+"/api/rooms/"+roomID+"/transfer/prepare", map[string]string{"to_device_id": candidateDeviceID}, &out, authHeader(token))
	return out, err
}

func (c *Client) CommitTransfer(ctx context.Context, candidateDeviceID, generation string, snapshotSeq uint64) error {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	return postJSON(ctx, c.http, c.server.BaseURL+"/api/rooms/"+roomID+"/transfer/commit", map[string]any{"to_device_id": candidateDeviceID, "generation": generation, "snapshot_seq": snapshotSeq}, nil, authHeader(token))
}

// RequestApproval is used by the owning device's agent runtime to surface
// a permission prompt to the current borrower (the session operator).
func (s *Session) RequestApproval(p borrowagent.ApprovalRequestPayload) error {
	return s.writeTyped("approval_request", p)
}

// DecideApproval answers a pending prompt; only the session operator's
// decision is accepted.
func (s *Session) DecideApproval(p borrowagent.ApprovalDecisionPayload) error {
	return s.writeTyped("approval_decision", p)
}

// Close ends the session socket.
func (s *Session) Close() error {
	s.cancel()
	return s.conn.Close(websocket.StatusNormalClosure, "")
}

// EnsureChunks uploads the missing chunks of a manifest to the server CAS.
func (c *Client) EnsureChunks(ctx context.Context, manifest vmcas.Manifest, chunks [][]byte) error {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	for i, hash := range manifest.Chunks {
		url := fmt.Sprintf("%s/api/rooms/%s/cas/%s", c.server.BaseURL, roomID, hash)
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		res, err := c.http.Do(req)
		if err != nil {
			return err
		}
		res.Body.Close()
		if res.StatusCode == http.StatusOK {
			continue
		}
		req, err = http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(chunks[i]))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		res, err = c.http.Do(req)
		if err != nil {
			return err
		}
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("chunk %s upload status %d", hash, res.StatusCode)
		}
	}
	// The manifest object itself is stored under its hash.
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		fmt.Sprintf("%s/api/rooms/%s/cas/%s", c.server.BaseURL, roomID, manifest.Hash), bytes.NewReader(manifest.Body()))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("manifest upload status %d", res.StatusCode)
	}
	return nil
}

// FetchChunk downloads one chunk into the local CAS cache.
func (c *Client) FetchChunk(ctx context.Context, hash string, cache vmcas.Store) error {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	url := fmt.Sprintf("%s/api/rooms/%s/cas/%s", c.server.BaseURL, roomID, hash)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("chunk %s status %d", hash, res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, vmcas.ChunkSize+1))
	if err != nil {
		return err
	}
	return cache.Put(hash, data)
}

// ResolveRoutes queries the gateway route registry.
func (c *Client) ResolveRoutes(ctx context.Context, query string) (map[string]any, error) {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	var out map[string]any
	err := getJSON(ctx, c.http, c.server.BaseURL+"/api/rooms/"+roomID+"/routes"+query, token, &out)
	return out, err
}

// RoomID returns the joined room's id.
func (c *Client) RoomID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.roomID
}

// LookupDevice implements gateway.RouteLookup: every session occupying
// (device slug, port) right now.
func (c *Client) LookupDevice(deviceSlug string, port int) ([]vmprotocol.SessionCandidate, error) {
	out, err := c.ResolveRoutes(context.Background(),
		fmt.Sprintf("?device=%s&port=%d", url.QueryEscape(deviceSlug), port))
	if err != nil {
		return nil, err
	}
	raw, _ := out["candidates"].([]any)
	cands := make([]vmprotocol.SessionCandidate, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		cand := vmprotocol.SessionCandidate{
			SessionID:     strValue(m["session_id"]),
			SessionLabel:  strValue(m["session_label"]),
			Agent:         strValue(m["agent"]),
			CanonicalHost: strValue(m["canonical_host"]),
		}
		if cand.SessionID != "" {
			cands = append(cands, cand)
		}
	}
	return cands, nil
}

// LookupSession implements gateway.RouteLookup: the unique session route
// for a full session address.
func (c *Client) LookupSession(deviceSlug, sessionID string, port int) (vmprotocol.SessionCandidate, error) {
	out, err := c.ResolveRoutes(context.Background(),
		fmt.Sprintf("?device=%s&port=%d&session=%s", url.QueryEscape(deviceSlug), port, url.QueryEscape(sessionID)))
	if err != nil {
		return vmprotocol.SessionCandidate{}, err
	}
	return vmprotocol.SessionCandidate{
		SessionID:     strValue(out["session_id"]),
		CanonicalHost: strValue(out["canonical_host"]),
	}, nil
}

func strValue(v any) string {
	s, _ := v.(string)
	return s
}

// RoomRoutes lists every live route in the room (gateway proxy sync).
// The server-computed slug rides along: raw device ids can differ from
// the enrolled-hostname slugs canonical .tutti hosts use, and gateways
// must bind hostnames by slug while dialing by raw id.
func (c *Client) RoomRoutes(ctx context.Context) ([]vmprotocol.LiveRoute, error) {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	var res struct {
		Routes []vmprotocol.LiveRoute `json:"routes"`
	}
	if err := getJSON(ctx, c.http, c.server.BaseURL+"/api/rooms/"+roomID+"/routes?list=1", token, &res); err != nil {
		return nil, err
	}
	out := make([]vmprotocol.LiveRoute, 0, len(res.Routes))
	for _, r := range res.Routes {
		r.RoomID = roomID
		out = append(out, r)
	}
	return out, nil
}

// Leave exits the room (owner path requires apply + disband/transfer done
// first, per the meeting rules).
func (c *Client) Leave(ctx context.Context, workspaceApplied, disband bool, workspaceBaseSeq uint64) error {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	return postJSON(ctx, c.http, c.server.BaseURL+"/api/rooms/"+roomID+"/leave",
		map[string]any{
			"workspace_applied":  workspaceApplied,
			"disband":            disband,
			"workspace_base_seq": workspaceBaseSeq,
		}, nil, authHeader(token))
}

func postJSON(ctx context.Context, hc *http.Client, url string, body any, out any, opts ...func(*http.Request)) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for _, opt := range opts {
		opt(req)
	}
	return doJSON(hc, req, out)
}

func getJSON(ctx context.Context, hc *http.Client, url, token string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return doJSON(hc, req, out)
}

func authHeader(token string) func(*http.Request) {
	return func(req *http.Request) {
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

func doJSON(hc *http.Client, req *http.Request, out any) error {
	res, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		json.NewDecoder(res.Body).Decode(&e)
		return fmt.Errorf("%s %s: %s", req.Method, req.URL.Path, e.Error)
	}
	if out != nil {
		return json.NewDecoder(res.Body).Decode(out)
	}
	io.Copy(io.Discard, res.Body)
	return nil
}
