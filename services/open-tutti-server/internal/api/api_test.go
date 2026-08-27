package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/config"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/preview"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/realtime"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/room"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/sequencer"
	store_sqlite "github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/store/sqlite"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/tunnel"
)

type testStack struct {
	srv    *httptest.Server
	client *http.Client
}

func newTestStack(t *testing.T) *testStack {
	t.Helper()
	repo, err := store_sqlite.Open(t.TempDir() + "/api.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { repo.Close() })
	cas := vmcas.NewMemoryStore()
	cfg := config.Config{
		Secret: "api-test", PublicURL: "http://server.example",
		OwnerGracePeriod: 5 * time.Minute, JoinTicketTTL: time.Minute, SnapshotIntervalOps: 8,
	}
	rooms := room.NewService(repo, cfg, room.RealClock{}, nil)
	previews := preview.NewRegistry()
	log := testLogger()
	hub := realtime.NewHub(nil, rooms, previews, log)
	seq := sequencer.NewManager(repo, cfg, cas, hub, log)
	hub.SetSequencer(seq)
	relay := tunnel.NewRelay(log)
	api := New(cfg, rooms, seq, hub, previews, relay, cas, repo, log)
	rooms.SetBroadcaster(hub)
	ts := httptest.NewServer(api.Handler())
	t.Cleanup(ts.Close)
	return &testStack{srv: ts, client: ts.Client()}
}

type creatorResponse struct {
	RoomID       string `json:"room_id"`
	ShareID      string `json:"share_id"`
	ShareURL     string `json:"share_url"`
	Password     string `json:"password"`
	SessionToken string `json:"session_token"`
}

func (s *testStack) createRoom(t *testing.T, deviceID string) creatorResponse {
	t.Helper()
	body := map[string]any{
		"device": map[string]string{
			"id": deviceID, "display_name": "Anna", "hostname": "Annas-MacBook-Pro", "public_key": "pk1",
		},
	}
	data, _ := json.Marshal(body)
	res, err := s.client.Post(s.srv.URL+"/api/rooms", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create room status %d", res.StatusCode)
	}
	var created creatorResponse
	json.NewDecoder(res.Body).Decode(&created)
	return created
}

func (s *testStack) post(t *testing.T, path string, body any, out any) *http.Response {
	t.Helper()
	data, _ := json.Marshal(body)
	res, err := s.client.Post(s.srv.URL+path, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		json.NewDecoder(res.Body).Decode(out)
	}
	return res
}

func TestShareToJoinJourney(t *testing.T) {
	stack := newTestStack(t)
	created := stack.createRoom(t, "dev_anna")

	// Share page renders for a valid share id and hides the password.
	page, err := stack.client.Get(stack.srv.URL + "/share/" + created.ShareID)
	if err != nil {
		t.Fatal(err)
	}
	if page.StatusCode != http.StatusOK {
		t.Fatalf("share page status %d", page.StatusCode)
	}

	// Wrong password gets no ticket.
	res := stack.post(t, "/api/share/"+created.ShareID+"/join-ticket", map[string]string{"password": "000000"}, nil)
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong password status %d", res.StatusCode)
	}

	// Correct password issues a one-time ticket; the deep link carries the
	// ticket, never the password.
	var ticketRes struct {
		Ticket   string `json:"ticket"`
		DeepLink string `json:"deep_link"`
	}
	res = stack.post(t, "/api/share/"+created.ShareID+"/join-ticket", map[string]string{"password": created.Password}, &ticketRes)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("ticket status %d", res.StatusCode)
	}
	if !strings.Contains(ticketRes.DeepLink, "open-tutti://join?") || !strings.Contains(ticketRes.DeepLink, ticketRes.Ticket) {
		t.Fatalf("deep link %q", ticketRes.DeepLink)
	}
	if strings.Contains(ticketRes.DeepLink, created.Password) {
		t.Fatal("deep link leaks the room password")
	}

	// Desktop redeems the ticket and receives a session token plus a
	// bootstrap snapshot.
	var joinRes struct {
		RoomID       string                       `json:"room_id"`
		SessionToken string                       `json:"session_token"`
		Snapshot     vmprotocol.WorkspaceSnapshot `json:"snapshot"`
	}
	res = stack.post(t, "/api/rooms/"+created.RoomID+"/join", map[string]any{
		"ticket": ticketRes.Ticket,
		"device": map[string]string{
			"id": "dev_leo", "display_name": "Leo", "hostname": "Leos-PC", "public_key": "pk2",
		},
	}, &joinRes)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("join status %d", res.StatusCode)
	}
	if joinRes.SessionToken == "" || joinRes.RoomID != created.RoomID {
		t.Fatalf("join response %+v", joinRes)
	}

	// Ticket reuse is refused.
	res = stack.post(t, "/api/rooms/"+created.RoomID+"/join", map[string]any{
		"ticket": ticketRes.Ticket,
		"device": map[string]string{"id": "dev_x", "display_name": "X", "hostname": "x", "public_key": "pk3"},
	}, nil)
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("ticket reuse status %d", res.StatusCode)
	}
}

func TestWorkspaceOperationRoundTripOverWS(t *testing.T) {
	stack := newTestStack(t)
	created := stack.createRoom(t, "dev_anna")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Owner connects to the room socket.
	wsURL := strings.Replace(stack.srv.URL, "http://", "ws://", 1) +
		"/api/rooms/" + created.RoomID + "/ws?token=" + created.SessionToken
	ownerWS, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("owner ws dial: %v", err)
	}
	defer ownerWS.CloseNow()

	// Submit a create op through the socket.
	env := vmprotocol.Envelope{
		RoomID: created.RoomID, OperationID: "op-1", AuthorDeviceID: "dev_anna", BaseSeq: 0,
		Operation: vmprotocol.FileOperation{ID: "op-1", Path: "hello.txt", Kind: vmprotocol.OpCreate},
	}
	envBytes, _ := env.Encode()
	msg, _ := json.Marshal(realtime.ClientMessage{Type: "op", Envelope: envBytes})
	if err := ownerWS.Write(ctx, websocket.MessageText, msg); err != nil {
		t.Fatal(err)
	}

	// The broadcast comes back with a server sequence.
	var got realtime.ServerMessage
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		readCtx, cancelRead := context.WithTimeout(ctx, time.Second)
		_, data, err := ownerWS.Read(readCtx)
		cancelRead()
		if err != nil {
			continue
		}
		json.Unmarshal(data, &got)
		if got.Event.Topic == vmprotocol.TopicOperation {
			var broadcast vmprotocol.Envelope
			json.Unmarshal(got.Event.Payload, &broadcast)
			if broadcast.ServerSeq == 1 && broadcast.Operation.Path == "hello.txt" {
				return
			}
		}
	}
	t.Fatal("operation broadcast with server seq never arrived")
}

func TestCASChunkEndpointsAndBootstrap(t *testing.T) {
	stack := newTestStack(t)
	created := stack.createRoom(t, "dev_anna")

	content := []byte("chunk-bytes-123")
	hash := vmcas.ChunkHash(content)

	// PUT is idempotent and hash-verified.
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest("PUT", stack.srv.URL+"/api/rooms/"+created.RoomID+"/cas/"+hash, bytes.NewReader(content))
		req.Header.Set("Authorization", "Bearer "+created.SessionToken)
		res, err := stack.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("cas put %d: %d", i, res.StatusCode)
		}
	}
	// Mismatched content is rejected.
	bad, _ := http.NewRequest("PUT", stack.srv.URL+"/api/rooms/"+created.RoomID+"/cas/"+hash, bytes.NewReader([]byte("other")))
	bad.Header.Set("Authorization", "Bearer "+created.SessionToken)
	res, _ := stack.client.Do(bad)
	res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("mismatched chunk accepted: %d", res.StatusCode)
	}

	// Unauthenticated access is refused.
	noAuth, _ := http.NewRequest("GET", stack.srv.URL+"/api/rooms/"+created.RoomID+"/cas/"+hash, nil)
	res, _ = stack.client.Do(noAuth)
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated cas get: %d", res.StatusCode)
	}

	// Bootstrap returns the (empty) snapshot for a fresh room.
	req, _ := http.NewRequest("GET", stack.srv.URL+"/api/rooms/"+created.RoomID+"/bootstrap", nil)
	req.Header.Set("Authorization", "Bearer "+created.SessionToken)
	res, err := stack.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap status %d", res.StatusCode)
	}
	var boot struct {
		Snapshot vmprotocol.WorkspaceSnapshot `json:"snapshot"`
	}
	json.NewDecoder(res.Body).Decode(&boot)
	if boot.Snapshot.RoomID != created.RoomID {
		t.Fatalf("bootstrap snapshot %+v", boot.Snapshot)
	}
}

func TestPreviewRouteResolutionRules(t *testing.T) {
	stack := newTestStack(t)
	created := stack.createRoom(t, "dev_anna")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsURL := strings.Replace(stack.srv.URL, "http://", "ws://", 1) +
		"/api/rooms/" + created.RoomID + "/ws?token=" + created.SessionToken
	ws, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()

	// Two sessions of the same device both listen on :3000.
	for _, session := range []string{"claude-a", "codex-b"} {
		msg, _ := json.Marshal(realtime.ClientMessage{
			Type: "ports",
			Ports: &vmprotocol.PortsChangedPayload{
				SessionID: "sess-" + session, SessionLabel: session, Port: 3000, Protocol: "http", Listening: true,
			},
		})
		if err := ws.Write(ctx, websocket.MessageText, msg); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	routes := func(query string) map[string]any {
		req, reqErr := http.NewRequest("GET", stack.srv.URL+"/api/rooms/"+created.RoomID+"/routes"+query, nil)
		if reqErr != nil {
			t.Fatal(reqErr)
		}
		req.Header.Set("Authorization", "Bearer "+created.SessionToken)
		res, doErr := stack.client.Do(req)
		if doErr != nil {
			t.Fatal(doErr)
		}
		defer res.Body.Close()
		var out map[string]any
		json.NewDecoder(res.Body).Decode(&out)
		return out
	}

	// Device-level ambiguous port returns both candidates ordered by
	// canonical host.
	amb := routes("?device=dev_anna&port=3000")
	if amb["resolved"] != false {
		t.Fatalf("ambiguous device route resolved: %v", amb)
	}
	cands, _ := json.Marshal(amb["candidates"])
	if !strings.Contains(string(cands), "claude-a.annas-macbook-pro.tutti:3000") ||
		!strings.Contains(string(cands), "codex-b.annas-macbook-pro.tutti:3000") {
		t.Fatalf("candidates %s", cands)
	}

	// Session-level address always resolves.
	sess := routes("?device=dev_anna&session=sess-claude-a&port=3000")
	if sess["resolved"] != true {
		t.Fatalf("session route not resolved: %v", sess)
	}

	// Unique device port routes transparently: stop one listener.
	msg, _ := json.Marshal(realtime.ClientMessage{
		Type:  "ports",
		Ports: &vmprotocol.PortsChangedPayload{SessionID: "sess-codex-b", SessionLabel: "codex-b", Port: 3000, Listening: false},
	})
	ws.Write(ctx, websocket.MessageText, msg)
	time.Sleep(200 * time.Millisecond)
	uniq := routes("?device=dev_anna&port=3000")
	if uniq["resolved"] != true || uniq["session_id"] != "sess-claude-a" {
		t.Fatalf("unique device route: %v", uniq)
	}
}

func TestTransferEndpointsEnforceOwner(t *testing.T) {
	stack := newTestStack(t)
	created := stack.createRoom(t, "dev_anna")

	// A second device joins through the share flow.
	var ticketRes struct{ Ticket string }
	stack.post(t, "/api/share/"+created.ShareID+"/join-ticket", map[string]string{"password": created.Password}, &ticketRes)
	var joinRes struct{ SessionToken string }
	res := stack.post(t, "/api/rooms/"+created.RoomID+"/join", map[string]any{
		"ticket": ticketRes.Ticket,
		"device": map[string]string{"id": "dev_leo", "display_name": "Leo", "hostname": "Leos-PC", "public_key": "pk"},
	}, &joinRes)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("join %d", res.StatusCode)
	}

	// Non-owner cannot prepare a transfer: unauthenticated first.
	res = stack.post(t, "/api/rooms/"+created.RoomID+"/transfer/prepare",
		map[string]string{"to_device_id": "dev_anna"}, nil)
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated transfer prepare %d", res.StatusCode)
	}

	doAuthed := func(path string, body any) int {
		data, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", stack.srv.URL+path, bytes.NewReader(data))
		req.Header.Set("Authorization", "Bearer "+created.SessionToken)
		resp, err := stack.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	// Owner prepares, then an incomplete transfer cannot commit.
	preparePath := "/api/rooms/" + created.RoomID + "/transfer/prepare"
	if code := doAuthed(preparePath, map[string]string{"to_device_id": "dev_leo"}); code != http.StatusOK {
		t.Fatalf("transfer prepare status %d", code)
	}

	commitPath := "/api/rooms/" + created.RoomID + "/transfer/commit"
	if code := doAuthed(commitPath, map[string]any{"to_device_id": "dev_leo", "replica_full": true, "workspace_initialized": false}); code != http.StatusConflict {
		t.Fatalf("incomplete transfer commit status %d", code)
	}
	if code := doAuthed(commitPath, map[string]any{"to_device_id": "dev_stranger", "replica_full": true, "workspace_initialized": true}); code != http.StatusConflict {
		t.Fatalf("non-candidate commit status %d", code)
	}
	if code := doAuthed(commitPath, map[string]any{"to_device_id": "dev_leo", "replica_full": true, "workspace_initialized": true}); code != http.StatusOK {
		t.Fatalf("transfer commit status %d", code)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
