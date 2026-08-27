package api

import (
	"net/http"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	"github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/types"
)

func registerTuttiModeExecutionRoutes(
	mux *http.ServeMux,
	wrapper *tuttigenerated.ServerInterfaceWrapper,
) {
	mux.HandleFunc("/v1/workspaces/{workspaceID}/tutti-executions/{issueID}/cancel-execution", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			types.WriteMethodNotAllowed(w)
			return
		}
		wrapper.CancelTuttiModeExecution(w, r)
	})
	mux.HandleFunc("/v1/workspaces/{workspaceID}/tutti-executions/{issueID}/archive", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			wrapper.GetTuttiModeArchiveOperation(w, r)
		case http.MethodPost:
			wrapper.ArchiveTuttiModeExecution(w, r)
		default:
			types.WriteMethodNotAllowed(w)
		}
	})
}
