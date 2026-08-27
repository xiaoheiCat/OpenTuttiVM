package issuemanager

import (
	"context"
	"net/url"
	"regexp"
	"strings"

	workspaceissues "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/issues"
)

// markdownLinkPattern matches `[label](href)` links in issue/task markdown content.
// Best-effort (mirrors the rich-text link form); file paths rarely contain `)` or `]`.
var markdownLinkPattern = regexp.MustCompile(`\[([^\]]*)\]\(([^)\s]+)\)`)

const (
	referenceSourceContext   = "context"
	referenceSourceContent   = "content"
	referenceSourceReference = "reference"

	workspaceReferenceHrefPrefix = "mention://workspace-reference/"
)

// issueReferenceFile is a flat, agent-readable reference surfaced on issue/task detail so the
// agent can read referenced input files as context without parsing markdown or resolving handles.
type issueReferenceFile struct {
	Path        string
	DisplayName string
	Source      string
}

// referenceHandle is an embedded `mention://workspace-reference/...` link awaiting expansion.
type referenceHandle struct {
	Source  string
	ID      string
	GroupID string
}

// collectIssueReferences flattens an issue's referenced input files: workspace file links in the
// content, attached context refs, and embedded workspace-reference handles expanded to their files.
func (p Provider) collectIssueReferences(ctx context.Context, workspaceID string, detail workspaceissues.IssueDetail) []issueReferenceFile {
	files, handles := parseContentReferences(detail.Issue.Content)
	files = append(files, contextRefReferences(detail.ContextRefs)...)
	files = append(files, p.expandReferenceHandles(ctx, workspaceID, handles)...)
	return dedupeReferences(files)
}

// collectTaskReferences mirrors collectIssueReferences for a task's content + context refs.
func (p Provider) collectTaskReferences(ctx context.Context, workspaceID string, detail workspaceissues.TaskDetail) []issueReferenceFile {
	files, handles := parseContentReferences(detail.Task.Content)
	files = append(files, contextRefReferences(detail.ContextRefs)...)
	files = append(files, p.expandReferenceHandles(ctx, workspaceID, handles)...)
	return dedupeReferences(files)
}

// expandReferenceHandles resolves embedded workspace-reference handles into their artifact files.
// Best-effort: a handle that fails to resolve is skipped so issue/task get stays healthy.
func (p Provider) expandReferenceHandles(ctx context.Context, workspaceID string, handles []referenceHandle) []issueReferenceFile {
	if len(handles) == 0 {
		return nil
	}
	resolver := p.referenceResolver()
	out := make([]issueReferenceFile, 0, len(handles))
	for _, handle := range handles {
		resolved, err := resolver.Resolve(ctx, workspaceID, handle.Source, handle.ID, handle.GroupID, "", 0)
		if err != nil {
			continue
		}
		for _, file := range resolved {
			out = append(out, issueReferenceFile{
				Path:        file.Path,
				DisplayName: file.DisplayName,
				Source:      referenceSourceReference,
			})
		}
	}
	return out
}

// parseContentReferences splits issue/task markdown links into direct workspace file references
// and embedded workspace-reference handles. Mentions (other than workspace-reference) and external
// links are dropped.
func parseContentReferences(content string) (files []issueReferenceFile, handles []referenceHandle) {
	for _, match := range markdownLinkPattern.FindAllStringSubmatch(content, -1) {
		href := strings.TrimSpace(match[2])
		if handle, ok := parseWorkspaceReferenceHandle(href); ok {
			handles = append(handles, handle)
			continue
		}
		if !isWorkspaceFileHref(href) {
			continue
		}
		name := strings.TrimSpace(match[1])
		if name == "" {
			name = referencePathBaseName(href)
		}
		files = append(files, issueReferenceFile{
			Path:        href,
			DisplayName: name,
			Source:      referenceSourceContent,
		})
	}
	return files, handles
}

func parseWorkspaceReferenceHandle(href string) (referenceHandle, bool) {
	if !strings.HasPrefix(strings.ToLower(href), workspaceReferenceHrefPrefix) {
		return referenceHandle{}, false
	}
	parsed, err := url.Parse(href)
	if err != nil {
		return referenceHandle{}, false
	}
	id := strings.Trim(parsed.Path, "/")
	if id == "" {
		return referenceHandle{}, false
	}
	query := parsed.Query()
	return referenceHandle{
		Source:  strings.TrimSpace(query.Get("source")),
		ID:      id,
		GroupID: strings.TrimSpace(query.Get("groupId")),
	}, true
}

func contextRefReferences(refs []workspaceissues.ContextRef) []issueReferenceFile {
	out := make([]issueReferenceFile, 0, len(refs))
	for _, ref := range refs {
		out = append(out, issueReferenceFile{
			Path:        ref.Path,
			DisplayName: ref.DisplayName,
			Source:      referenceSourceContext,
		})
	}
	return out
}

func isWorkspaceFileHref(href string) bool {
	if href == "" {
		return false
	}
	lower := strings.ToLower(href)
	if strings.HasPrefix(lower, "mention://") ||
		strings.HasPrefix(lower, "data:") ||
		strings.HasPrefix(href, "//") {
		return false
	}
	// Reject any explicit scheme (http://, https://, file://, ...).
	if idx := strings.Index(href, "://"); idx > 0 {
		return false
	}
	return true
}

func referencePathBaseName(path string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(path), "/")
	if idx := strings.LastIndex(trimmed, "/"); idx >= 0 {
		return trimmed[idx+1:]
	}
	return trimmed
}

// dedupeReferences keeps the first occurrence per path (content links precede context refs,
// which precede handle-expanded files).
func dedupeReferences(refs []issueReferenceFile) []issueReferenceFile {
	seen := make(map[string]struct{}, len(refs))
	out := make([]issueReferenceFile, 0, len(refs))
	for _, ref := range refs {
		path := strings.TrimSpace(ref.Path)
		if path == "" {
			continue
		}
		if _, dup := seen[path]; dup {
			continue
		}
		seen[path] = struct{}{}
		ref.Path = path
		out = append(out, ref)
	}
	return out
}

func referenceFileValues(refs []issueReferenceFile) []any {
	values := make([]any, 0, len(refs))
	for _, ref := range refs {
		values = append(values, map[string]any{
			"path":        ref.Path,
			"displayName": ref.DisplayName,
			"source":      ref.Source,
		})
	}
	return values
}
