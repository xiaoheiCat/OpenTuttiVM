package vmsync

import "testing"

func TestDiffTextMultibyteSuffixDeletion(t *testing.T) {
	// Removing an ASCII suffix right after a multibyte rune once indexed
	// past the end of the shorter side and panicked.
	patch, ok := DiffText([]byte("🙂a"), []byte("🙂"))
	if !ok {
		t.Fatal("expected text diff")
	}
	if len(patch.Splices) != 1 {
		t.Fatalf("splices %+v", patch.Splices)
	}
	s := patch.Splices[0]
	if s.Offset != 4 || s.DeleteLen != 1 || s.Insert != "" {
		t.Fatalf("splice %+v", s)
	}
}
