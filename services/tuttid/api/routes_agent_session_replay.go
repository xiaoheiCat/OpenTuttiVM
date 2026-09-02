package api

import (
	"net/http"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	tuttitypes "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/types"
)

func registerAgentSessionReplayRoutes(
	mux *http.ServeMux,
	wrapper *tuttigenerated.ServerInterfaceWrapper,
) {
	mux.HandleFunc("/v1/workspaces/{workspaceID}/agent-session-cassettes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			tuttitypes.WriteMethodNotAllowed(w)
			return
		}
		wrapper.ListAgentSessionCassettes(w, r)
	})
	mux.HandleFunc("/v1/workspaces/{workspaceID}/agent-session-cassettes/import", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			tuttitypes.WriteMethodNotAllowed(w)
			return
		}
		wrapper.ImportAgentSessionCassettes(w, r)
	})
	mux.HandleFunc("/v1/agent-session-replay/cassettes/{cassetteID}/transport/verify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			tuttitypes.WriteMethodNotAllowed(w)
			return
		}
		wrapper.VerifyAgentSessionReplayTransport(w, r)
	})
	mux.HandleFunc("/v1/agent-session-replay/cassettes/{cassetteID}/transport/playback", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			wrapper.GetAgentSessionReplayTransportPlayback(w, r)
		case http.MethodPost:
			wrapper.UpdateAgentSessionReplayTransportPlayback(w, r)
		default:
			tuttitypes.WriteMethodNotAllowed(w)
		}
	})
	mux.HandleFunc("/v1/agent-session-replay/cassettes/{cassetteID}/checkpoints/{checkpointIndex}/verify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			tuttitypes.WriteMethodNotAllowed(w)
			return
		}
		wrapper.VerifyAgentSessionReplayCheckpoint(w, r)
	})
	mux.HandleFunc("/v1/workspaces/{workspaceID}/agent-session-replay-workspaces", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			tuttitypes.WriteMethodNotAllowed(w)
			return
		}
		wrapper.PrepareAgentSessionReplayWorkspace(w, r)
	})
}
