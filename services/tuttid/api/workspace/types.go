package workspace

import (
	"time"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
)

func GeneratedSummaryFromBiz(value workspacebiz.Summary) tuttigenerated.WorkspaceSummary {
	var lastOpenedAt *time.Time
	if value.LastOpenedAt != nil {
		formatted := value.LastOpenedAt.UTC()
		lastOpenedAt = &formatted
	}

	return tuttigenerated.WorkspaceSummary{
		Id:           value.ID,
		LastOpenedAt: lastOpenedAt,
		Name:         value.Name,
	}
}

func GeneratedSummariesFromBiz(items []workspacebiz.Summary) []tuttigenerated.WorkspaceSummary {
	if len(items) == 0 {
		return []tuttigenerated.WorkspaceSummary{}
	}

	result := make([]tuttigenerated.WorkspaceSummary, 0, len(items))
	for _, item := range items {
		result = append(result, GeneratedSummaryFromBiz(item))
	}

	return result
}

func GeneratedStartupResponseFromBiz(item *workspacebiz.Summary) tuttigenerated.StartupWorkspaceResponse {
	if item == nil {
		return tuttigenerated.StartupWorkspaceResponse{Workspace: nil}
	}

	summary := GeneratedSummaryFromBiz(*item)
	return tuttigenerated.StartupWorkspaceResponse{Workspace: &summary}
}

func GeneratedEnvelopeResponseFromBiz(item workspacebiz.Summary) tuttigenerated.WorkspaceResponse {
	return tuttigenerated.WorkspaceResponse{
		Workspace: GeneratedSummaryFromBiz(item),
	}
}
