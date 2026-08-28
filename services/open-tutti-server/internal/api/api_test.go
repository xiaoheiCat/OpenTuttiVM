package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	vmagent "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-agent"
	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/borrow"
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
	hub := realtime.NewHub(nil, rooms, previews, borrow.NewRegistry(), log)
	seq := sequencer.NewManager(repo, cfg, cas, hub, log)
	hub.SetSequencer(seq)
	relay := tunnel.NewRelay(log, previews)
	api := New(cfg, rooms, seq, hub, previews, borrow.NewRegistry(), relay, cas, repo, log)
	rooms.SetBroadcaster(hub)
	ts := httptest.NewServer(api.Handler())
	// Runs FIRST (LIFO): close the listener, then wait for every hijacked
	// business-socket pump to finish its detach writes, and only then
	// (repo cleanup, registered earlier) does the store close — on
	// Windows a racing store close leaves api.db held open and TempDir
	// cleanup fails.
	t.Cleanup(func() {
		ts.Close()
		api.WaitIdle()
	})
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

	// Correct password issues a one-time ticket; the deep link uses the
	// desktop-registered tutti:// scheme, carries room + ticket (join
	// redemption needs the room id), and never the password.
	var ticketRes struct {
		Ticket   string `json:"ticket"`
		RoomID   string `json:"room_id"`
		DeepLink string `json:"deep_link"`
	}
	res = stack.post(t, "/api/share/"+created.ShareID+"/join-ticket", map[string]string{"password": created.Password}, &ticketRes)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("ticket status %d", res.StatusCode)
	}
	if !strings.Contains(ticketRes.DeepLink, "tutti://join?") ||
		!strings.Contains(ticketRes.DeepLink, "room="+url.QueryEscape(ticketRes.RoomID)) ||
		!strings.Contains(ticketRes.DeepLink, ticketRes.Ticket) {
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
	// canonical host. The WS announce is asynchronous: poll until the
	// registry reflects both listeners (a fixed sleep flaked on
	// slower CI runners).
	var cands []byte
	for i := 0; i < 50; i++ {
		amb := routes("?device=dev_anna&port=3000")
		if amb["resolved"] != false {
			t.Fatalf("ambiguous device route resolved: %v", amb)
		}
		cands, _ = json.Marshal(amb["candidates"])
		if strings.Contains(string(cands), "claude-a.annas-macbook-pro.tutti:3000") &&
			strings.Contains(string(cands), "codex-b.annas-macbook-pro.tutti:3000") {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !strings.Contains(string(cands), "claude-a.annas-macbook-pro.tutti:3000") ||
		!strings.Contains(string(cands), "codex-b.annas-macbook-pro.tutti:3000") {
		t.Fatalf("candidates %s", cands)
	}

	// Session-level address always resolves.
	sess := routes("?device=dev_anna&session=sess-claude-a&port=3000")
	if sess["resolved"] != true {
		t.Fatalf("session route not resolved: %v", sess)
	}

	// Unique device port routes transparently: stop one listener. Poll
	// for the de-registration (asynchronous, see above).
	msg, _ := json.Marshal(realtime.ClientMessage{
		Type:  "ports",
		Ports: &vmprotocol.PortsChangedPayload{SessionID: "sess-codex-b", SessionLabel: "codex-b", Port: 3000, Listening: false},
	})
	ws.Write(ctx, websocket.MessageText, msg)
	var uniq map[string]any
	for i := 0; i < 50; i++ {
		uniq = routes("?device=dev_anna&port=3000")
		if uniq["resolved"] == true && uniq["session_id"] == "sess-claude-a" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
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

// TestOwnerPrivilegesRevokeShareAndKick covers the two remaining owner
// powers from the design record: revoking the share link stops new joins,
// and kicking a member kills their session immediately.
func TestOwnerPrivilegesRevokeShareAndKick(t *testing.T) {
	stack := newTestStack(t)
	created := stack.createRoom(t, "dev_alice")

	// Bob joins once (membership + session token).
	var ticketRes struct {
		Ticket string `json:"ticket"`
	}
	stack.post(t, "/api/share/"+created.ShareID+"/join-ticket", map[string]string{"password": created.Password}, &ticketRes)
	var joinRes struct {
		SessionToken string `json:"session_token"`
	}
	res := stack.post(t, "/api/rooms/"+created.RoomID+"/join", map[string]any{
		"ticket": ticketRes.Ticket,
		"device": map[string]string{"id": "dev_bob", "display_name": "Bob", "hostname": "bobs-pc", "public_key": "pk7"},
	}, &joinRes)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("bob join status %d", res.StatusCode)
	}

	// Non-owner cannot revoke or kick.
	auth := func(path, token string) *http.Response {
		req, _ := http.NewRequest("POST", stack.srv.URL+path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		res, err := stack.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res
	}
	if code := auth("/api/rooms/"+created.RoomID+"/share/revoke", joinRes.SessionToken).StatusCode; code != http.StatusForbidden {
		t.Fatalf("non-owner revoke status %d", code)
	}
	if code := auth("/api/rooms/"+created.RoomID+"/members/dev_bob/kick", joinRes.SessionToken).StatusCode; code != http.StatusForbidden {
		t.Fatalf("non-owner kick status %d", code)
	}

	// Owner cannot kick themselves.
	if code := auth("/api/rooms/"+created.RoomID+"/members/dev_alice/kick", created.SessionToken).StatusCode; code != http.StatusForbidden {
		t.Fatalf("self-kick status %d", code)
	}

	// Owner kicks Bob: his session token dies with the membership row.
	if code := auth("/api/rooms/"+created.RoomID+"/members/dev_bob/kick", created.SessionToken).StatusCode; code != http.StatusOK {
		t.Fatalf("kick status %d", code)
	}
	boot, _ := http.NewRequest("GET", stack.srv.URL+"/api/rooms/"+created.RoomID+"/bootstrap", nil)
	boot.Header.Set("Authorization", "Bearer "+joinRes.SessionToken)
	res2, err := stack.client.Do(boot)
	if err != nil {
		t.Fatal(err)
	}
	res2.Body.Close()
	if res2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("kicked member bootstrap status %d", res2.StatusCode)
	}

	// Owner revokes the share link: the share page no longer mints tickets.
	if code := auth("/api/rooms/"+created.RoomID+"/share/revoke", created.SessionToken).StatusCode; code != http.StatusOK {
		t.Fatalf("revoke status %d", code)
	}
	res3 := stack.post(t, "/api/share/"+created.ShareID+"/join-ticket", map[string]string{"password": created.Password}, nil)
	if res3.StatusCode != http.StatusGone {
		t.Fatalf("post-revoke ticket status %d", res3.StatusCode)
	}

	// The still-member owner keeps access after revocation.
	own, _ := http.NewRequest("GET", stack.srv.URL+"/api/rooms/"+created.RoomID+"/bootstrap", nil)
	own.Header.Set("Authorization", "Bearer "+created.SessionToken)
	res4, err := stack.client.Do(own)
	if err != nil {
		t.Fatal(err)
	}
	res4.Body.Close()
	if res4.StatusCode != http.StatusOK {
		t.Fatalf("owner bootstrap after revoke status %d", res4.StatusCode)
	}
}

// wsReadUntil reads from a room socket until an event with the given topic
// arrives (or the deadline passes) and decodes its payload.
func wsReadUntil(t *testing.T, ctx context.Context, ws *websocket.Conn, topic vmprotocol.EventTopic, out any) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		readCtx, cancelRead := context.WithTimeout(ctx, 300*time.Millisecond)
		_, data, err := ws.Read(readCtx)
		cancelRead()
		if err != nil {
			continue
		}
		var msg realtime.ServerMessage
		if json.Unmarshal(data, &msg) != nil || msg.Event.Topic != topic {
			continue
		}
		if out != nil {
			if err := json.Unmarshal(msg.Event.Payload, out); err != nil {
				t.Fatalf("decode %s payload: %v", topic, err)
			}
		}
		return
	}
	t.Fatalf("event %s never arrived", topic)
}

func wsWrite(t *testing.T, ctx context.Context, ws *websocket.Conn, msg realtime.ClientMessage) {
	t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatal(err)
	}
}

// TestAgentBorrowingFlow walks the locked borrowing semantics end to end:
// owner shares, borrower commands, the owner's runtime prompts, the
// borrower (session operator) decides, and revocation fences stale
// generations.
func TestAgentBorrowingFlow(t *testing.T) {
	stack := newTestStack(t)
	created := stack.createRoom(t, "dev_alice")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Bob joins through the share → ticket → join path.
	var ticketRes struct {
		Ticket string `json:"ticket"`
	}
	stack.post(t, "/api/share/"+created.ShareID+"/join-ticket", map[string]string{"password": created.Password}, &ticketRes)
	var joinRes struct {
		SessionToken string `json:"session_token"`
	}
	res := stack.post(t, "/api/rooms/"+created.RoomID+"/join", map[string]any{
		"ticket": ticketRes.Ticket,
		"device": map[string]string{"id": "dev_bob", "display_name": "Bob", "hostname": "bobs-pc", "public_key": "pk9"},
	}, &joinRes)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("bob join status %d", res.StatusCode)
	}

	dial := func(token string) *websocket.Conn {
		wsURL := strings.Replace(stack.srv.URL, "http://", "ws://", 1) +
			"/api/rooms/" + created.RoomID + "/ws?token=" + token
		ws, _, err := websocket.Dial(ctx, wsURL, nil)
		if err != nil {
			t.Fatalf("ws dial: %v", err)
		}
		t.Cleanup(func() { ws.CloseNow() })
		return ws
	}
	aliceWS, bobWS := dial(created.SessionToken), dial(joinRes.SessionToken)

	// Alice shares her Claude Code instance (BorrowSafe satisfied).
	share := vmagent.AgentSharedPayload{
		AgentInstanceID: "agent-claude-1", Provider: "claude-code", Borrowable: true, Shared: true,
		Capabilities: vmagent.AgentCapabilities{Skills: []string{"repo-walk"}, MCP: []string{"github"}},
	}
	wsWrite(t, ctx, aliceWS, realtime.ClientMessage{Type: "agent_share", AgentShare: &share})
	var shared vmagent.AgentSharedPayload
	wsReadUntil(t, ctx, aliceWS, vmagent.TopicAgentShared, &shared)
	if !shared.Shared || shared.LeaseGeneration != 1 || shared.OwnerDeviceID != "dev_alice" {
		t.Fatalf("shared payload %+v", shared)
	}

	// Bob commands the shared agent on the live lease.
	cmd := vmagent.BorrowCommandPayload{
		CommandID: "c1", AgentInstanceID: "agent-claude-1",
		LeaseGeneration: shared.LeaseGeneration, Input: "look at issue 12",
	}
	wsWrite(t, ctx, bobWS, realtime.ClientMessage{Type: "borrow_command", BorrowCommand: &cmd})
	var routed vmagent.BorrowCommandPayload
	wsReadUntil(t, ctx, aliceWS, vmagent.TopicBorrowCommand, &routed)
	if routed.BorrowerDeviceID != "dev_bob" || routed.Input != "look at issue 12" {
		t.Fatalf("routed command %+v", routed)
	}

	// The agent runtime on Alice's device hits a permission prompt; it
	// routes to Bob, the session operator — never to Alice.
	prompt := vmagent.ApprovalRequestPayload{
		ApprovalID: "ap1", AgentInstanceID: "agent-claude-1", Provider: "claude-code",
		Prompt: "Run `gh pr view 12`?", Options: []string{"allow once", "deny"},
	}
	wsWrite(t, ctx, aliceWS, realtime.ClientMessage{Type: "approval_request", ApprovalRequest: &prompt})
	var request vmagent.ApprovalRequestPayload
	wsReadUntil(t, ctx, bobWS, vmagent.TopicApprovalRequest, &request)
	if request.SessionOperatorDeviceID != "dev_bob" || request.ApprovalID != "ap1" {
		t.Fatalf("approval request %+v", request)
	}

	// Bob decides; the decision routes back to Alice with Bob as decider.
	decision := vmagent.ApprovalDecisionPayload{ApprovalID: "ap1", AgentInstanceID: "agent-claude-1", Choice: 0}
	wsWrite(t, ctx, bobWS, realtime.ClientMessage{Type: "approval_decision", ApprovalDecision: &decision})
	var decided vmagent.ApprovalDecisionPayload
	wsReadUntil(t, ctx, aliceWS, vmagent.TopicApprovalDecision, &decided)
	if decided.DeciderDeviceID != "dev_bob" || decided.Choice != 0 {
		t.Fatalf("decision %+v", decided)
	}

	// Alice revokes: generation bumps and every old-generation command
	// dies immediately.
	wsWrite(t, ctx, aliceWS, realtime.ClientMessage{
		Type:       "agent_share",
		AgentShare: &vmagent.AgentSharedPayload{AgentInstanceID: "agent-claude-1", Shared: false},
	})
	var revoked vmagent.BorrowRevokedPayload
	wsReadUntil(t, ctx, bobWS, vmagent.TopicBorrowRevoked, &revoked)
	if revoked.FinalGeneration != 2 {
		t.Fatalf("revoked payload %+v", revoked)
	}
	stale := cmd
	stale.CommandID = "c2"
	wsWrite(t, ctx, bobWS, realtime.ClientMessage{Type: "borrow_command", BorrowCommand: &stale})
	wsReadUntil(t, ctx, bobWS, vmagent.TopicBorrowRevoked, &revoked)
	if revoked.AgentInstanceID != "agent-claude-1" {
		t.Fatalf("stale command notice %+v", revoked)
	}
}
