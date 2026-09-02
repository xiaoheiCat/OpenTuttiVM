// Package refresolve resolves a workspace-reference handle (app+group / topic+issue) into a
// flat list of artifact files. It is the shared core behind the `reference list` CLI command and
// the inline reference expansion surfaced on issue/task detail, so both go through one egress.
package refresolve

import (
	"context"
	"errors"
	"strings"

	workspaceissues "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/issues"
	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
)

const (
	// SourceApp / SourceTask are the two reference handle sources.
	SourceApp  = "app"
	SourceTask = "task"

	// MaxFiles bounds a single resolution (defence against cycles / pathologically large trees);
	// also the default when no limit is provided.
	MaxFiles = 1000
	// pageLimit is the per-page size when paging an app's hierarchy.
	pageLimit = 200
)

// ErrInvalidSource is returned when a handle source is neither "app" nor "task".
var ErrInvalidSource = errors.New("invalid reference input: source must be 'app' or 'task'")

// AppReferences is the unified egress to a workspace app's produced artifacts.
// Satisfied by *workspaceservice.AppCenterService.
type AppReferences interface {
	ListReferences(context.Context, string, string, workspacebiz.AppReferenceListInput) (workspacebiz.AppReferenceListResult, error)
}

// IssueOutputs is the unified egress to task (issue / topic) produced artifacts.
// Satisfied by workspaceservice.IssueManagerService.
type IssueOutputs interface {
	GetIssueDetail(context.Context, string, string) (workspaceissues.IssueDetail, error)
	SearchIssueOutputs(context.Context, workspaceissues.RunOutputSearchParams) ([]workspaceissues.RunOutputSearchHit, error)
}

// File is the normalized, source-agnostic shape both branches map into.
type File struct {
	Path            string
	DisplayName     string
	SizeBytes       int64
	MediaType       string
	CreatedAtUnixMs int64
}

// Resolver expands reference handles using the same in-process egress the desktop picker uses.
type Resolver struct {
	Apps   AppReferences
	Issues IssueOutputs
}

// Resolve flattens a single reference handle into its artifact files. limit <= 0 means MaxFiles.
func (r Resolver) Resolve(ctx context.Context, workspaceID, source, id, groupID, query string, limit int) ([]File, error) {
	if limit <= 0 || limit > MaxFiles {
		limit = MaxFiles
	}
	switch strings.TrimSpace(source) {
	case SourceApp:
		return r.collectAppFiles(ctx, workspaceID, id, groupID, query, limit)
	case SourceTask:
		return r.collectTaskFiles(ctx, workspaceID, id, groupID, query, limit)
	default:
		return nil, ErrInvalidSource
	}
}

// collectAppFiles walks the app's reference hierarchy (group → children) breadth-first and
// flattens it to files, going through the unified AppCenterService.ListReferences egress.
func (r Resolver) collectAppFiles(ctx context.Context, workspaceID, appID, groupID, query string, limit int) ([]File, error) {
	if r.Apps == nil {
		return nil, ErrInvalidSource
	}
	files := make([]File, 0, limit)
	seenFiles := make(map[string]struct{})
	visitedGroups := make(map[string]struct{})
	queue := []string{strings.TrimSpace(groupID)}
	for len(queue) > 0 && len(files) < limit {
		current := queue[0]
		queue = queue[1:]
		if _, ok := visitedGroups[current]; ok {
			continue
		}
		visitedGroups[current] = struct{}{}

		cursor := ""
		for len(files) < limit {
			result, err := r.Apps.ListReferences(ctx, workspaceID, appID, workspacebiz.AppReferenceListInput{
				ParentGroupID: current,
				FilterText:    strings.TrimSpace(query),
				Limit:         pageLimit,
				Cursor:        cursor,
				Kinds:         []workspacebiz.AppReferenceKind{workspacebiz.AppReferenceKindFile},
			})
			if err != nil {
				return nil, err
			}
		collectItems:
			for _, item := range result.Items {
				switch item.AppReferenceListItemType() {
				case workspacebiz.AppReferenceListItemTypeGroup:
					if group, ok := item.(workspacebiz.AppReferenceGroup); ok {
						queue = append(queue, group.ID)
					}
				case workspacebiz.AppReferenceListItemTypeReference:
					ref, ok := item.(workspacebiz.AppReferenceListReferenceItem)
					if !ok {
						continue
					}
					file, ok := ref.Reference.(workspacebiz.AppFileReference)
					if !ok {
						continue
					}
					if _, dup := seenFiles[file.Path]; dup {
						continue
					}
					seenFiles[file.Path] = struct{}{}
					files = append(files, File{
						Path:            file.Path,
						DisplayName:     file.DisplayName,
						SizeBytes:       derefInt64(file.SizeBytes),
						MediaType:       file.MimeType,
						CreatedAtUnixMs: derefInt64(file.MtimeMs),
					})
					if len(files) >= limit {
						break collectItems
					}
				}
			}
			cursor = strOrEmpty(result.NextCursor)
			if cursor == "" {
				break
			}
		}
	}
	return files, nil
}

// collectTaskFiles resolves task artifacts through the in-process IssueManagerService:
// a specific issue (groupId) → its latest outputs; whole topic (id, no groupId) → search.
func (r Resolver) collectTaskFiles(ctx context.Context, workspaceID, topicID, issueID, query string, limit int) ([]File, error) {
	if r.Issues == nil {
		return nil, ErrInvalidSource
	}
	files := make([]File, 0, limit)

	if trimmedIssueID := strings.TrimSpace(issueID); trimmedIssueID != "" {
		detail, err := r.Issues.GetIssueDetail(ctx, workspaceID, trimmedIssueID)
		if err != nil {
			return nil, err
		}
		needle := strings.ToLower(strings.TrimSpace(query))
		for _, out := range detail.LatestOutputs {
			if needle != "" && !strings.Contains(strings.ToLower(out.DisplayName), needle) {
				continue
			}
			files = append(files, outputToFile(out))
			if len(files) >= limit {
				break
			}
		}
		return files, nil
	}

	hits, err := r.Issues.SearchIssueOutputs(ctx, workspaceissues.RunOutputSearchParams{
		WorkspaceID: workspaceID,
		TopicID:     strings.TrimSpace(topicID),
		Query:       strings.TrimSpace(query),
		Limit:       limit,
	})
	if err != nil {
		return nil, err
	}
	for _, hit := range hits {
		files = append(files, outputToFile(hit.Output))
		if len(files) >= limit {
			break
		}
	}
	return files, nil
}

func outputToFile(out workspaceissues.RunOutput) File {
	return File{
		Path:            out.Path,
		DisplayName:     out.DisplayName,
		SizeBytes:       out.SizeBytes,
		MediaType:       out.MediaType,
		CreatedAtUnixMs: out.CreatedAtUnixMS,
	}
}

func derefInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func strOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}
