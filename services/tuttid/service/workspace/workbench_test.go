package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	workspacebiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/workspace"
	workspacedata "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/data/workspace"
)

type stubWorkbenchStore struct {
	getWorkbenchSnapshotFn func(context.Context, string) (workspacebiz.WorkbenchSnapshot, error)
	putWorkbenchSnapshotFn func(context.Context, workspacebiz.WorkbenchSnapshot) error
}

func (s stubWorkbenchStore) GetWorkbenchSnapshot(ctx context.Context, workspaceID string) (workspacebiz.WorkbenchSnapshot, error) {
	if s.getWorkbenchSnapshotFn == nil {
		return workspacebiz.WorkbenchSnapshot{}, nil
	}

	return s.getWorkbenchSnapshotFn(ctx, workspaceID)
}

func (s stubWorkbenchStore) PutWorkbenchSnapshot(ctx context.Context, snapshot workspacebiz.WorkbenchSnapshot) error {
	if s.putWorkbenchSnapshotFn == nil {
		return nil
	}

	return s.putWorkbenchSnapshotFn(ctx, snapshot)
}

type stubWorkbenchTerminalLister struct {
	sessions []TerminalSession
	err      error
}

func (s stubWorkbenchTerminalLister) List(context.Context, string) ([]TerminalSession, error) {
	return s.sessions, s.err
}

type stubWorkbenchSnapshotReconciler struct {
	called bool
}

func (s *stubWorkbenchSnapshotReconciler) ReconcileSnapshot(_ context.Context, snapshot workspacebiz.WorkbenchSnapshot) (workspacebiz.WorkbenchSnapshot, error) {
	s.called = true
	snapshot.JSON = []byte(`{"schemaVersion":1,"nodes":[]}`)
	return snapshot, nil
}

func TestWorkbenchServiceReturnsDefaultSnapshotOnStoreMiss(t *testing.T) {
	t.Parallel()

	service := WorkbenchService{
		Store: stubWorkbenchStore{
			getWorkbenchSnapshotFn: func(context.Context, string) (workspacebiz.WorkbenchSnapshot, error) {
				return workspacebiz.WorkbenchSnapshot{}, workspacedata.ErrWorkbenchSnapshotNotFound
			},
		},
	}

	snapshot, err := service.GetSnapshot(context.Background(), "workspace-1")
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	if snapshot.WorkspaceID != "workspace-1" {
		t.Fatalf("WorkspaceID = %q, want workspace-1", snapshot.WorkspaceID)
	}
	if snapshot.SchemaVersion != 1 {
		t.Fatalf("SchemaVersion = %d, want 1", snapshot.SchemaVersion)
	}
	if string(snapshot.JSON) == "" {
		t.Fatal("JSON = empty, want default snapshot JSON")
	}
}

func TestWorkbenchServiceFiltersMissingTerminalNodesFromStoredSnapshot(t *testing.T) {
	t.Parallel()

	service := WorkbenchService{
		Store: stubWorkbenchStore{
			getWorkbenchSnapshotFn: func(context.Context, string) (workspacebiz.WorkbenchSnapshot, error) {
				return workspacebiz.WorkbenchSnapshot{
					WorkspaceID:   "workspace-1",
					SchemaVersion: 1,
					JSON: mustWorkbenchSnapshotJSON(t, WorkbenchSnapshot{
						SchemaVersion: 1,
						Nodes: []WorkbenchSnapshotNode{
							{
								ID:    "files",
								Kind:  "workspace-files",
								Title: "Files",
								Frame: validWorkbenchSnapshotFrame(),
							},
							{
								ID:    "terminal:missing-term",
								Kind:  "terminal",
								Title: "Terminal",
								Frame: validWorkbenchSnapshotFrame(),
								Data: map[string]interface{}{
									"instanceId":  "missing-term",
									"instanceKey": "workspace-terminal",
									"typeId":      "workspace-terminal",
								},
							},
						},
						NodeStack:    stringSlicePointer([]string{"files", "terminal:missing-term"}),
						ActiveNodeID: stringPointer("terminal:missing-term"),
						Spaces: &[]WorkbenchSnapshotSpace{
							{
								ID:      "space-1",
								Name:    "Main",
								NodeIDs: []string{"files", "terminal:missing-term"},
							},
						},
						ActiveSpaceID: stringPointer("space-1"),
						LockedLayout: &WorkbenchSnapshotLockedLayout{
							Preset: WorkbenchSnapshotLayoutPreset{
								Kind: WorkbenchSnapshotLayoutPresetKind("row"),
							},
							NodeIDs: []string{"files", "terminal:missing-term"},
						},
					}),
				}, nil
			},
		},
		SnapshotReconciler: TerminalWorkbenchSnapshotReconciler{
			TerminalService: stubWorkbenchTerminalLister{},
		},
	}

	snapshot, err := service.GetSnapshot(context.Background(), "workspace-1")
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}

	var decoded WorkbenchSnapshot
	if err := json.Unmarshal(snapshot.JSON, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(decoded.Nodes) != 1 || decoded.Nodes[0].ID != "files" {
		t.Fatalf("nodes = %#v, want only files node", decoded.Nodes)
	}
	if decoded.NodeStack == nil || len(*decoded.NodeStack) != 1 || (*decoded.NodeStack)[0] != "files" {
		t.Fatalf("NodeStack = %#v, want [files]", decoded.NodeStack)
	}
	if decoded.ActiveNodeID == nil || *decoded.ActiveNodeID != "files" {
		t.Fatalf("ActiveNodeID = %#v, want files", decoded.ActiveNodeID)
	}
	if decoded.Spaces == nil || len(*decoded.Spaces) != 1 {
		t.Fatalf("Spaces = %#v, want one space", decoded.Spaces)
	}
	if nodeIDs := (*decoded.Spaces)[0].NodeIDs; len(nodeIDs) != 1 || nodeIDs[0] != "files" {
		t.Fatalf("space node IDs = %#v, want [files]", (*decoded.Spaces)[0].NodeIDs)
	}
	if decoded.LockedLayout != nil {
		t.Fatalf("LockedLayout = %#v, want nil after terminal removal", decoded.LockedLayout)
	}
}

func TestWorkbenchServiceUsesSnapshotReconcilerSeam(t *testing.T) {
	t.Parallel()

	reconciler := &stubWorkbenchSnapshotReconciler{}
	service := WorkbenchService{
		SnapshotReconciler: reconciler,
		Store: stubWorkbenchStore{
			getWorkbenchSnapshotFn: func(context.Context, string) (workspacebiz.WorkbenchSnapshot, error) {
				return workspacebiz.WorkbenchSnapshot{
					WorkspaceID:   "workspace-1",
					SchemaVersion: 1,
					JSON:          mustWorkbenchSnapshotJSON(t, WorkbenchSnapshot{SchemaVersion: 1, Nodes: []WorkbenchSnapshotNode{}}),
				}, nil
			},
		},
	}

	snapshot, err := service.GetSnapshot(context.Background(), "workspace-1")
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	if !reconciler.called {
		t.Fatal("custom reconciler was not called")
	}
	if string(snapshot.JSON) != `{"schemaVersion":1,"nodes":[]}` {
		t.Fatalf("JSON = %s, want custom reconciler JSON", snapshot.JSON)
	}
}

func TestWorkbenchServiceWrapsInvalidSnapshotError(t *testing.T) {
	t.Parallel()

	service := WorkbenchService{
		Store: stubWorkbenchStore{},
	}

	_, err := service.PutSnapshot(context.Background(), "workspace-1", WorkbenchSnapshot{})
	if !errors.Is(err, ErrInvalidWorkbenchSnapshot) {
		t.Fatalf("PutSnapshot() error = %v, want ErrInvalidWorkbenchSnapshot", err)
	}
}

func mustWorkbenchSnapshotJSON(t *testing.T, snapshot WorkbenchSnapshot) []byte {
	t.Helper()

	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return data
}

func stringPointer(value string) *string {
	return &value
}

func stringSlicePointer(value []string) *[]string {
	return &value
}

func validWorkbenchSnapshotFrame() WorkbenchSnapshotFrame {
	return WorkbenchSnapshotFrame{
		X:      0,
		Y:      0,
		Width:  160,
		Height: 120,
	}
}
