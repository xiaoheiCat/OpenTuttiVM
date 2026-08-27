package issuemanager

import (
	"context"
	"testing"

	workspaceissues "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/issues"
)

// fakeIssueManagerWithOutputs overrides GetIssueDetail to drive task-source handle expansion,
// reusing the full fakeIssueManager for every other method.
type fakeIssueManagerWithOutputs struct {
	*fakeIssueManager
	detail workspaceissues.IssueDetail
}

func (f fakeIssueManagerWithOutputs) GetIssueDetail(context.Context, string, string) (workspaceissues.IssueDetail, error) {
	return f.detail, nil
}

func TestParseContentReferencesSplitsFilesAndHandles(t *testing.T) {
	content := "See [spec.md](/workspace/docs/spec.md) and folder [assets](/workspace/assets/). " +
		"Handoff [@Issue](mention://workspace-issue/i1). Project " +
		"[@Proto](mention://workspace-reference/topic-x?source=task&groupId=issue-y). " +
		"External [site](https://example.com/x)."

	files, handles := parseContentReferences(content)

	if len(files) != 2 ||
		files[0] != (issueReferenceFile{Path: "/workspace/docs/spec.md", DisplayName: "spec.md", Source: referenceSourceContent}) ||
		files[1] != (issueReferenceFile{Path: "/workspace/assets/", DisplayName: "assets", Source: referenceSourceContent}) {
		t.Fatalf("files = %+v", files)
	}
	if len(handles) != 1 ||
		handles[0] != (referenceHandle{Source: "task", ID: "topic-x", GroupID: "issue-y"}) {
		t.Fatalf("handles = %+v", handles)
	}
}

func TestParseContentReferencesUsesBaseNameWhenLabelEmpty(t *testing.T) {
	files, _ := parseContentReferences("[](/workspace/docs/readme.md)")
	if len(files) != 1 || files[0].DisplayName != "readme.md" {
		t.Fatalf("files = %+v", files)
	}
}

func TestCollectIssueReferencesExpandsEmbeddedHandle(t *testing.T) {
	fake := fakeIssueManagerWithOutputs{
		fakeIssueManager: &fakeIssueManager{},
		detail: workspaceissues.IssueDetail{LatestOutputs: []workspaceissues.RunOutput{
			{Path: "/workspace/out/login.html", DisplayName: "login.html", MediaType: "text/html"},
		}},
	}
	provider := NewProvider(fakeWorkspaceCatalog{}, fake, nil)

	detail := workspaceissues.IssueDetail{
		Issue: workspaceissues.Issue{Content: "[spec.md](/workspace/docs/spec.md) " +
			"[@Proto](mention://workspace-reference/topic-x?source=task&groupId=issue-y)"},
		ContextRefs: []workspaceissues.ContextRef{
			{Path: "/workspace/data/in.csv", DisplayName: "in.csv"},
		},
	}

	refs := provider.collectIssueReferences(context.Background(), "ws-1", detail)

	// content link, then context ref, then handle-expanded artifact file.
	want := []issueReferenceFile{
		{Path: "/workspace/docs/spec.md", DisplayName: "spec.md", Source: referenceSourceContent},
		{Path: "/workspace/data/in.csv", DisplayName: "in.csv", Source: referenceSourceContext},
		{Path: "/workspace/out/login.html", DisplayName: "login.html", Source: referenceSourceReference},
	}
	if len(refs) != len(want) {
		t.Fatalf("got %d references, want %d: %+v", len(refs), len(want), refs)
	}
	for i, ref := range refs {
		if ref != want[i] {
			t.Fatalf("reference[%d] = %+v, want %+v", i, ref, want[i])
		}
	}
}
