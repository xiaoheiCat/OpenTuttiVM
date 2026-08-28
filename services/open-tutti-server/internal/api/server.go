// Package api assembles the open-tutti-server HTTP surface: room lifecycle
// REST, the share join page, CAS chunk endpoints, route resolution for the
// .tutti gateway, the business WebSocket, and the multiplexed tunnel.
package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/coder/websocket"
	vmcas "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas"
	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
	vmsync "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync"

	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/borrow"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/config"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/preview"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/realtime"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/room"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/sequencer"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/store"
	"github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server/internal/tunnel"
)

// Server is the HTTP composition root.
type Server struct {
	cfg      config.Config
	rooms    *room.Service
	seq      *sequencer.Manager
	hub      *realtime.Hub
	previews *preview.Registry
	borrows  *borrow.Registry
	relay    *tunnel.Relay
	cas      vmcas.Store
	repo     store.Repository
	log      *slog.Logger
}

// New wires the API server.
func New(cfg config.Config, rooms *room.Service, seq *sequencer.Manager, hub *realtime.Hub,
	previews *preview.Registry, borrows *borrow.Registry, relay *tunnel.Relay, cas vmcas.Store, repo store.Repository, log *slog.Logger) *Server {
	return &Server{cfg: cfg, rooms: rooms, seq: seq, hub: hub, previews: previews, borrows: borrows, relay: relay, cas: cas, repo: repo, log: log}
}

// WaitIdle blocks until every business-socket pump finished its detach
// sequence — embedders and tests call it after closing the HTTP listener
// and before closing the store, so the final membership writes cannot
// race the store close.
func (s *Server) WaitIdle() { s.hub.WaitPumps() }

// Handler builds the route table.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/info", s.handleInfo)
	mux.HandleFunc("POST /api/rooms", s.handleCreateRoom)
	mux.HandleFunc("GET /share/{shareID}", s.handleSharePage)
	mux.HandleFunc("POST /api/share/{shareID}/join-ticket", s.handleJoinTicket)
	mux.HandleFunc("POST /api/rooms/{roomID}/join", func(w http.ResponseWriter, r *http.Request) {
		s.handleJoin(w, r, r.PathValue("roomID"), "")
	})
	mux.HandleFunc("POST /api/rooms/{roomID}/leave", s.authRoom(s.handleLeave))
	mux.HandleFunc("POST /api/rooms/{roomID}/password/rotate", s.authRoomOwner(s.handleRotatePassword))
	mux.HandleFunc("POST /api/rooms/{roomID}/share/revoke", s.authRoomOwner(s.handleRevokeShareLink))
	mux.HandleFunc("POST /api/rooms/{roomID}/members/{deviceID}/kick", s.authRoomOwner(s.handleKickMember))
	mux.HandleFunc("GET /api/rooms/{roomID}/bootstrap", s.authRoom(s.handleBootstrap))
	mux.HandleFunc("PUT /api/rooms/{roomID}/cas/{hash}", s.authRoom(s.handleCASPut))
	mux.HandleFunc("HEAD /api/rooms/{roomID}/cas/{hash}", s.authRoom(s.handleCASHead))
	mux.HandleFunc("GET /api/rooms/{roomID}/cas/{hash}", s.authRoom(s.handleCASGet))
	mux.HandleFunc("GET /api/rooms/{roomID}/routes", s.authRoom(s.handleRoutes))
	mux.HandleFunc("POST /api/rooms/{roomID}/transfer/prepare", s.authRoomOwner(s.handleTransferPrepare))
	mux.HandleFunc("POST /api/rooms/{roomID}/transfer/commit", s.authRoomOwner(s.handleTransferCommit))
	mux.HandleFunc("POST /api/rooms/{roomID}/transfer/abort", s.authRoomOwner(s.handleTransferAbort))
	mux.HandleFunc("GET /api/rooms/{roomID}/ws", s.authRoomWS(s.handleRoomWS))
	mux.HandleFunc("GET /api/tunnel", s.handleTunnelWS)
	return mux
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"server_id":       serverID(s.cfg.Secret),
		"invite_required": s.cfg.ServerInviteCode != "",
		"version":         "1",
	})
}

type createRoomRequest struct {
	InviteCode string           `json:"invite_code"`
	Device     room.DeviceInput `json:"device"`
}

func (s *Server) handleCreateRoom(w http.ResponseWriter, r *http.Request) {
	var req createRoomRequest
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	created, err := s.rooms.CreateRoom(r.Context(), room.CreateRoomInput{
		InviteCode: req.InviteCode, Device: req.Device,
	})
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, room.ErrInviteRequired), errors.Is(err, room.ErrInviteWrong):
			status = http.StatusForbidden
		case strings.Contains(err.Error(), "required"):
			status = http.StatusBadRequest
		}
		writeErr(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

type joinTicketRequest struct {
	Password string `json:"password"`
}

func (s *Server) handleJoinTicket(w http.ResponseWriter, r *http.Request) {
	var req joinTicketRequest
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ticket, expiresAt, err := s.rooms.IssueJoinTicket(r.Context(), r.PathValue("shareID"), req.Password)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		} else if strings.Contains(err.Error(), "revoked") {
			status = http.StatusGone
		} else if strings.Contains(err.Error(), "too many attempts") {
			status = http.StatusTooManyRequests
		} else if strings.Contains(err.Error(), "password") {
			status = http.StatusUnauthorized
		}
		writeErr(w, status, err.Error())
		return
	}
	// The join redeem API needs the room id alongside the ticket, and the
	// deep link uses the scheme the packaged desktop actually registers
	// ("tutti://") so following the share page can hand off.
	room, err := s.repo.GetRoomByShareID(r.Context(), r.PathValue("shareID"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ticket":     ticket,
		"expires_at": expiresAt,
		"room_id":    room.ID,
		"deep_link": "tutti://join?server=" + url.QueryEscape(s.cfg.PublicURL) +
			"&room=" + url.QueryEscape(room.ID) + "&ticket=" + url.QueryEscape(ticket),
	})
}

type joinRequest struct {
	Ticket string           `json:"ticket"`
	Device room.DeviceInput `json:"device"`
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request, roomID, _ string) {
	var req joinRequest
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	joinedRoom, token, err := s.rooms.JoinRedeem(r.Context(), req.Ticket, req.Device)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	snap, ops, err := s.seq.Bootstrap(r.Context(), joinedRoom)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"room_id":       joinedRoom,
		"session_token": token,
		"snapshot":      snap,
		"ops":           ops,
	})
}

type leaveRequest struct {
	WorkspaceApplied bool `json:"workspace_applied"`
	Disband          bool `json:"disband"`
}

func (s *Server) handleLeave(w http.ResponseWriter, r *http.Request, roomID, deviceID string) {
	var req leaveRequest
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	err := s.rooms.Leave(r.Context(), room.LeaveInput{
		RoomID: roomID, DeviceID: deviceID,
		WorkspaceApplied: req.WorkspaceApplied, Disband: req.Disband,
	})
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		writeErr(w, status, err.Error())
		return
	}
	// Only a dissolved room tears down engine state; a participant
	// leaving while the room lives must not reset the workspace.
	if room, err := s.rooms.GetRoom(r.Context(), roomID); err == nil && room.DissolvedAt != nil {
		s.seq.CloseRoom(roomID)
		s.previews.ClearRoom(roomID)
		s.borrows.ClearRoom(roomID)
		// Dissolution is terminal: drop every remaining live socket and
		// tunnel so nothing sequences past the room's end.
		s.hub.DropRoom(roomID)
		s.relay.DropRoom(roomID)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "left"})
}

func (s *Server) handleRotatePassword(w http.ResponseWriter, r *http.Request, roomID, deviceID string) {
	password, err := s.rooms.RotatePassword(r.Context(), roomID, deviceID)
	if err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"password": password})
}

func (s *Server) handleRevokeShareLink(w http.ResponseWriter, r *http.Request, roomID, deviceID string) {
	if err := s.rooms.RevokeShareLink(r.Context(), roomID, deviceID); err != nil {
		status := http.StatusForbidden
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		writeErr(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "share_link_revoked"})
}

func (s *Server) handleKickMember(w http.ResponseWriter, r *http.Request, roomID, deviceID string) {
	target := r.PathValue("deviceID")
	if err := s.rooms.KickMember(r.Context(), roomID, deviceID, target); err != nil {
		status := http.StatusForbidden
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		writeErr(w, status, err.Error())
		return
	}
	// Deleting the membership stops future authentication; live sockets
	// and tunnels must die too, or the kicked device keeps operating.
	s.hub.DropDevice(roomID, target)
	s.relay.DropDevice(roomID, target)
	writeJSON(w, http.StatusOK, map[string]string{"status": "kicked", "device_id": target})
}

func (s *Server) handleBootstrap(w http.ResponseWriter, r *http.Request, roomID, _ string) {
	snap, ops, err := s.seq.Bootstrap(r.Context(), roomID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshot": snap, "ops": ops})
}

type transferPrepareRequest struct {
	To string `json:"to_device_id"`
}

func (s *Server) handleTransferPrepare(w http.ResponseWriter, r *http.Request, roomID, deviceID string) {
	var req transferPrepareRequest
	if err := readJSON(r, &req); err != nil || req.To == "" {
		writeErr(w, http.StatusBadRequest, "to_device_id required")
		return
	}
	if _, err := s.seq.SnapshotForTransfer(r.Context(), roomID); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.rooms.PrepareTransfer(r.Context(), roomID, deviceID, req.To); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "prepared"})
}

type transferCommitRequest struct {
	To                   string `json:"to_device_id"`
	ReplicaFull          bool   `json:"replica_full"`
	WorkspaceInitialized bool   `json:"workspace_initialized"`
}

func (s *Server) handleTransferCommit(w http.ResponseWriter, r *http.Request, roomID, deviceID string) {
	var req transferCommitRequest
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.rooms.CommitTransfer(r.Context(), roomID, deviceID, req.To, req.ReplicaFull, req.WorkspaceInitialized); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "transferred"})
}

func (s *Server) handleTransferAbort(w http.ResponseWriter, r *http.Request, roomID, deviceID string) {
	if err := s.rooms.AbortTransfer(r.Context(), roomID, deviceID); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "aborted"})
}

func (s *Server) handleCASPut(w http.ResponseWriter, r *http.Request, roomID, _ string) {
	hash := r.PathValue("hash")
	body, err := io.ReadAll(io.LimitReader(r.Body, vmcas.ChunkSize+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(body) > vmcas.ChunkSize {
		writeErr(w, http.StatusRequestEntityTooLarge, "chunk exceeds 4 MiB")
		return
	}
	if err := s.cas.Put(hash, body); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.repo.AddCASRefs(r.Context(), roomID, []string{hash}); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"hash": hash})
}

func (s *Server) handleCASGet(w http.ResponseWriter, r *http.Request, roomID, _ string) {
	hash := r.PathValue("hash")
	// CAS is process-global; authorize the read against the room's own
	// object references so one room's member cannot pull another room's
	// bytes by predicting a hash.
	refOK, err := s.repo.HasCASRef(r.Context(), roomID, hash)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !refOK {
		writeErr(w, http.StatusNotFound, "object not referenced by this room")
		return
	}
	data, err := s.cas.Get(hash)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(data)
}

func (s *Server) handleCASHead(w http.ResponseWriter, r *http.Request, roomID, _ string) {
	hash := r.PathValue("hash")
	refOK, err := s.repo.HasCASRef(r.Context(), roomID, hash)
	if err != nil || !refOK {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	ok, err := s.cas.Has(hash)
	if err != nil || !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleRoutes answers the room-sync gateway's .tutti resolution: a
// session-level address resolves directly; a device-level short address
// follows the occupancy rules (unique → route, ambiguous → candidate list
// for the HTTP H5 selector; raw TCP callers fail client-side).
func (s *Server) handleRoutes(w http.ResponseWriter, r *http.Request, roomID, _ string) {
	q := r.URL.Query()
	if q.Get("list") == "1" {
		// Every live route in the room: the gateway proxy reconciles its
		// synthetic-VIP listeners from this list.
		routes := []map[string]any{}
		for _, e := range s.previews.RoomSessions(roomID) {
			routes = append(routes, map[string]any{
				"device_id": e.DeviceID, "session_id": e.SessionID, "port": e.Port,
				"device_slug": e.DeviceSlug, "canonical_host": canonicalHost(e),
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"routes": routes})
		return
	}
	device := q.Get("device")
	port := 0
	fmt.Sscanf(q.Get("port"), "%d", &port)
	if device == "" || port == 0 {
		writeErr(w, http.StatusBadRequest, "device and port required")
		return
	}
	if session := q.Get("session"); session != "" {
		entry, ok := s.previews.SessionRoute(roomID, device, session, port)
		if !ok {
			writeErr(w, http.StatusNotFound, "session route not registered")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"resolved": true, "session_id": entry.SessionID, "canonical_host": canonicalHost(entry)})
		return
	}
	candidates := s.previews.DeviceRoutes(roomID, device, port)
	res, _ := vmprotocol.ResolveDeviceRoute(port, candidates)
	writeJSON(w, http.StatusOK, map[string]any{
		"resolved":   res.Resolved,
		"session_id": res.SessionID,
		"candidates": candidates,
	})
}

func (s *Server) handleRoomWS(w http.ResponseWriter, r *http.Request, roomID, deviceID string) {
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	// A valid text_patch envelope can carry up to MaxTextFile (8 MiB) of
	// inserted text; the library default of 32 KiB would close every
	// large paste as message-too-big before the sequencer sees it.
	ws.SetReadLimit(int64(vmsync.MaxTextFile) + 64<<10)
	if err := s.rooms.MarkOnline(r.Context(), roomID, deviceID); err != nil {
		ws.Close(websocket.StatusPolicyViolation, "membership offline")
		return
	}
	device, err := s.repo.GetDevice(r.Context(), deviceID)
	slug := "device"
	if err == nil {
		slug = vmprotocol.SlugifyHostname(device.Hostname)
	}
	conn := realtime.NewConn(r.Context(), roomID, deviceID, slug)
	s.hub.BroadcastRoom(roomID, vmprotocol.Event{
		Topic:  vmprotocol.TopicPresence,
		RoomID: roomID,
		Payload: mustJSON(vmprotocol.PresenceDevice{
			DeviceID: deviceID, Online: true, IsOwner: s.isOwner(r, roomID, deviceID),
		}),
	})
	s.hub.Handle(conn, ws)
}

func (s *Server) handleTunnelWS(w http.ResponseWriter, r *http.Request) {
	token := bearerOrQuery(r)
	roomID, deviceID, err := s.rooms.ValidateSessionToken(r.Context(), token)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	if err := s.relay.ServeTunnel(r.Context(), ws, roomID, deviceID); err != nil {
		s.log.Warn("tunnel closed", "room", roomID, "device", deviceID, "err", err)
	}
}

func (s *Server) isOwner(r *http.Request, roomID, deviceID string) bool {
	roomInfo, err := s.repo.GetRoom(r.Context(), roomID)
	return err == nil && roomInfo.OwnerDeviceID == deviceID
}

// roomHandler needs an authenticated membership.
type roomHandler func(w http.ResponseWriter, r *http.Request, roomID, deviceID string)

func (s *Server) authRoom(h roomHandler) http.HandlerFunc {
	return s.authToken(func(w http.ResponseWriter, r *http.Request, roomID, deviceID string) {
		h(w, r, roomID, deviceID)
	})
}

// authRoomWS authenticates and marks online inside the handler (after the
// websocket upgrade is possible), keeping WS connect presence correct.
func (s *Server) authRoomWS(h roomHandler) http.HandlerFunc {
	return s.authToken(h)
}

func (s *Server) authToken(h roomHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		roomID := r.PathValue("roomID")
		token := bearerOrQuery(r)
		authRoom, deviceID, err := s.rooms.ValidateSessionToken(r.Context(), token)
		if err != nil || authRoom != roomID {
			writeErr(w, http.StatusUnauthorized, "invalid session token")
			return
		}
		h(w, r, roomID, deviceID)
	}
}

func (s *Server) authRoomOwner(h roomHandler) http.HandlerFunc {
	return s.authToken(func(w http.ResponseWriter, r *http.Request, roomID, deviceID string) {
		roomInfo, err := s.repo.GetRoom(r.Context(), roomID)
		if err != nil || roomInfo.OwnerDeviceID != deviceID {
			writeErr(w, http.StatusForbidden, "room owner required")
			return
		}
		h(w, r, roomID, deviceID)
	})
}

func bearerOrQuery(r *http.Request) string {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	return r.URL.Query().Get("token")
}

func serverID(secret string) string {
	sum := sha256.Sum256([]byte("open-tutti-server-id:" + secret))
	return "srv_" + hex.EncodeToString(sum[:8])
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 4<<20))
	return dec.Decode(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func canonicalHost(e preview.Entry) string {
	return e.SessionLabel + "." + e.DeviceSlug + ".tutti:" + fmt.Sprint(e.Port)
}
