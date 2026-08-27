package collabrun

import (
	"context"
	"strings"

	collabrunbiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/collabrun"
	modelpolicyservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/modelpolicy"
)

// RunPolicyReviewConsult executes a policy-triggered review as a consult run
// so it shares the collaboration accounting, events, and timeline.
func (s *Service) RunPolicyReviewConsult(ctx context.Context, input modelpolicyservice.ReviewConsultInput) (modelpolicyservice.ReviewConsultResult, error) {
	run, err := s.StartConsult(ctx, StartConsultInput{
		WorkspaceID:     input.WorkspaceID,
		SourceSessionID: input.SourceSession,
		ModelPlanID:     input.ModelPlanID,
		Model:           input.Model,
		Question:        input.Question,
		TriggerSource:   string(collabrunbiz.TriggerPolicy),
		TriggerReason:   input.TriggerReason,
		MaxTokens:       input.MaxTokens,
	})
	if err != nil {
		return modelpolicyservice.ReviewConsultResult{}, err
	}
	return modelpolicyservice.ReviewConsultResult{
		RunID:       run.ID,
		ResultText:  run.ResultText,
		Failed:      run.Status != collabrunbiz.StatusCompleted,
		TotalTokens: run.Usage.InputTokens + run.Usage.OutputTokens,
	}, nil
}

// SumPolicyReviewUsage reports how many policy-triggered consult runs the
// session has consumed and their summed token usage, backing the review
// rule's run and token budgets.
func (s *Service) SumPolicyReviewUsage(ctx context.Context, workspaceID string, sourceSessionID string) (int, int64, error) {
	runs, err := s.ListRuns(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(sourceSessionID), 0)
	if err != nil {
		return 0, 0, err
	}
	count := 0
	var totalTokens int64
	for _, run := range runs {
		if run.Mode != collabrunbiz.ModeConsult || run.TriggerSource != collabrunbiz.TriggerPolicy {
			continue
		}
		count++
		totalTokens += run.Usage.InputTokens + run.Usage.OutputTokens
	}
	return count, totalTokens, nil
}

func (s *Service) HasPolicyReviewForTurn(ctx context.Context, workspaceID string, sourceSessionID string, turnID string) (bool, error) {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return false, nil
	}
	runs, err := s.ListRuns(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(sourceSessionID), 0)
	if err != nil {
		return false, err
	}
	suffix := ":turn:" + turnID
	for _, run := range runs {
		if run.Mode == collabrunbiz.ModeConsult &&
			run.TriggerSource == collabrunbiz.TriggerPolicy &&
			strings.HasPrefix(run.TriggerReason, "review_rule:") &&
			strings.HasSuffix(run.TriggerReason, suffix) {
			return true, nil
		}
	}
	return false, nil
}
