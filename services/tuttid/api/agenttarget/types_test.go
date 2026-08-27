package agenttarget

import (
	"strings"
	"testing"

	agenttargetbiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/agenttarget"
)

func TestGeneratedListAgentTargetsResponseFromBizSkipsInvalidTargets(t *testing.T) {
	defaultTarget := agenttargetbiz.DefaultSystemTargets(0)[0]
	response, err := GeneratedListAgentTargetsResponseFromBiz([]agenttargetbiz.Target{
		{
			ID:            "broken-target",
			Provider:      "codex",
			LaunchRefJSON: `{"type":"local_cli","provider":"claude-code"}`,
			Name:          "Broken Target",
			Enabled:       true,
			Source:        agenttargetbiz.SourceUser,
		},
		defaultTarget,
	})
	if err != nil {
		t.Fatalf("GeneratedListAgentTargetsResponseFromBiz() error = %v", err)
	}
	if len(response.Targets) != 1 {
		t.Fatalf("response targets len = %d, want 1", len(response.Targets))
	}
	if response.Targets[0].Id != defaultTarget.ID {
		t.Fatalf("response target id = %q, want %s", response.Targets[0].Id, defaultTarget.ID)
	}
}

func TestGeneratedAgentTargetFromBizProjectsMaskIconURL(t *testing.T) {
	target := agenttargetbiz.DefaultSystemTargets(0)[0]
	target.MaskIconURL = " data:image/svg+xml;base64,mask "

	generated, err := GeneratedAgentTargetFromBiz(target)
	if err != nil {
		t.Fatal(err)
	}
	if generated.MaskIconUrl == nil || *generated.MaskIconUrl != strings.TrimSpace(target.MaskIconURL) {
		t.Fatalf("generated mask icon URL = %#v", generated.MaskIconUrl)
	}
}
