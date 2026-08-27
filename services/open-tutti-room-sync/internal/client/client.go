// Package client is room-sync's connection to open-tutti-server: the
// business WebSocket with reconnect and sequence-gap resync, ticket
// redemption, and CAS chunk fetch/upload helpers.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"
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

// New creates a client.
func New(server Server, deviceID string) *Client {
	return &Client{server: server, http: &http.Client{}, deviceID: deviceID}
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
func (c *Client) Bootstrap(ctx context.Context) (vmprotocol.WorkspaceSnapshot, []vmprotocol.Envelope, error) {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	var res struct {
		Snapshot vmprotocol.WorkspaceSnapshot `json:"snapshot"`
		Ops      []vmprotocol.Envelope        `json:"ops"`
	}
	err := getJSON(ctx, c.http, c.server.BaseURL+"/api/rooms/"+roomID+"/bootstrap", token, &res)
	return res.Snapshot, res.Ops, err
}

// Session is one business WebSocket connection.
type Session struct {
	conn   *websocket.Conn
	client *Client

	// OnEvent receives every server event (operations, presence, conflicts,
	// ports, room ending).
	OnEvent func(ev vmprotocol.Event)

	// OnGap is called when a sequence gap requires a bootstrap resync; the
	// loop pauses until it returns.
	OnGap func() error

	ctx    context.Context
	cancel context.CancelFunc
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
	wsURL += "/api/rooms/" + roomID + "/ws?token=" + token
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return nil, err
	}
	sctx, cancel := context.WithCancel(ctx)
	return &Session{conn: conn, client: c, ctx: sctx, cancel: cancel}, nil
}

// Run pumps events until the socket dies. It applies envelope events to
// the replica, resyncing via OnGap on sequence gaps.
func (s *Session) Run(replica *vmsync.Replica) error {
	for {
		_, data, err := s.conn.Read(s.ctx)
		if err != nil {
			return err
		}
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
		if s.OnEvent != nil {
			s.OnEvent(msg.Event)
		}
		if msg.Event.Topic != vmprotocol.TopicOperation {
			continue
		}
		var env vmprotocol.Envelope
		if err := json.Unmarshal(msg.Event.Payload, &env); err != nil {
			continue
		}
		if _, err := replica.ApplyServerOp(env); err == vmsync.ErrSeqGap {
			if s.OnGap != nil {
				if err := s.OnGap(); err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}
}

// Submit sends one local operation.
func (s *Session) Submit(env vmprotocol.Envelope) error {
	envBytes, err := env.Encode()
	if err != nil {
		return err
	}
	msg, err := json.Marshal(map[string]any{"type": "op", "envelope": json.RawMessage(envBytes)})
	if err != nil {
		return err
	}
	return s.conn.Write(s.ctx, websocket.MessageText, msg)
}

// AnnouncePorts publishes a listening port for a local session.
func (s *Session) AnnouncePorts(p vmprotocol.PortsChangedPayload) error {
	msg, err := json.Marshal(map[string]any{"type": "ports", "ports": p})
	if err != nil {
		return err
	}
	return s.conn.Write(s.ctx, websocket.MessageText, msg)
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

// RoomRoutes lists every live route in the room (gateway proxy sync).
func (c *Client) RoomRoutes(ctx context.Context) ([]vmprotocol.RouteKey, error) {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	var res struct {
		Routes []struct {
			DeviceID  string `json:"device_id"`
			SessionID string `json:"session_id"`
			Port      int    `json:"port"`
		} `json:"routes"`
	}
	if err := getJSON(ctx, c.http, c.server.BaseURL+"/api/rooms/"+roomID+"/routes?list=1", token, &res); err != nil {
		return nil, err
	}
	out := make([]vmprotocol.RouteKey, 0, len(res.Routes))
	for _, r := range res.Routes {
		out = append(out, vmprotocol.RouteKey{RoomID: roomID, DeviceID: r.DeviceID, SessionID: r.SessionID, Port: r.Port})
	}
	return out, nil
}

// Leave exits the room (owner path requires apply + disband/transfer done
// first, per the meeting rules).
func (c *Client) Leave(ctx context.Context, workspaceApplied, disband bool) error {
	c.mu.Lock()
	token, roomID := c.token, c.roomID
	c.mu.Unlock()
	return postJSON(ctx, c.http, c.server.BaseURL+"/api/rooms/"+roomID+"/leave",
		map[string]bool{"workspace_applied": workspaceApplied, "disband": disband}, nil, authHeader(token))
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
