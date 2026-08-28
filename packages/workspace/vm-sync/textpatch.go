// Package vmsync implements the OpenTuttiVM collaboration engine: UTF-8
// byte-range text patches with server-authoritative transformation, the
// in-memory workspace state machine shared by the server sequencer, and the
// replica projection used by room-sync.
package vmsync

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"unicode/utf8"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// ContentHash returns the canonical content hash for file bytes.
func ContentHash(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// MaxTextFile is the size guard for OT-tracked text files; larger or binary
// content falls back to CAS blob replacement.
const MaxTextFile = 8 * 1024 * 1024

// DiffText computes a TextPatch transforming old into next. Content that is
// not valid UTF-8 or exceeds the size guard is reported as not text so
// callers fall back to blob replacement. The patch trims the common prefix
// and suffix, keeping splice boundaries on rune boundaries, and expresses the
// remainder as one middle-replacement splice.
func DiffText(old, next []byte) (*vmprotocol.TextPatch, bool) {
	if len(old) > MaxTextFile || len(next) > MaxTextFile {
		return nil, false
	}
	if !utf8.Valid(old) || !utf8.Valid(next) {
		return nil, false
	}
	prefix := commonPrefixLen(old, next)
	suffix := commonSuffixLen(old[prefix:], next[prefix:])

	delStart := prefix
	delEnd := len(old) - suffix
	insStart := prefix
	insEnd := len(next) - suffix

	delEnd, insEnd = alignRuneBackward(old, delEnd, next, insEnd)
	delStart, insStart = alignRuneForward(old, delStart, next, insStart)

	patch := &vmprotocol.TextPatch{BaseHash: ContentHash(old)}
	deleted := len(old[delStart:delEnd])
	inserted := string(next[insStart:insEnd])
	if deleted == 0 && inserted == "" {
		patch.Splices = []vmprotocol.Splice{}
		return patch, true
	}
	patch.Splices = []vmprotocol.Splice{{
		Offset:    delStart,
		DeleteLen: deleted,
		Insert:    inserted,
	}}
	return patch, true
}

func commonPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func commonSuffixLen(a, b []byte) int {
	i := 0
	for i < len(a) && i < len(b) && a[len(a)-1-i] == b[len(b)-1-i] {
		i++
	}
	return i
}

// alignRuneBackward pushes end offsets forward onto rune boundaries in both
// documents so a splice never splits a multi-byte rune.
func alignRuneBackward(old []byte, delEnd int, next []byte, insEnd int) (int, int) {
	for delEnd < len(old) && insEnd < len(next) &&
		(!utf8.RuneStart(old[delEnd]) || !utf8.RuneStart(next[insEnd])) {
		delEnd++
		insEnd++
	}
	return delEnd, insEnd
}

// alignRuneForward pushes start offsets back onto rune boundaries in both
// documents. insStart may already sit at len(next) (pure deletion at the
// end), so the guard must check both bounds before indexing.
func alignRuneForward(old []byte, delStart int, next []byte, insStart int) (int, int) {
	for delStart > 0 && insStart > 0 && delStart < len(old) && insStart < len(next) &&
		(!utf8.RuneStart(old[delStart]) || !utf8.RuneStart(next[insStart])) {
		delStart--
		insStart--
	}
	return delStart, insStart
}

// ValidateSplices checks that splices are ordered, non-overlapping, and
// within baseLen. Splices are parallel ranges against the base content.
func ValidateSplices(splices []vmprotocol.Splice, baseLen int) error {
	prevEnd := 0
	for i, s := range splices {
		if s.Offset < 0 || s.DeleteLen < 0 {
			return fmt.Errorf("splice %d: negative offset or length", i)
		}
		// Subtraction form: Offset+DeleteLen can overflow int and wrap
		// below baseLen, which would let ApplyPatch slice out of range.
		if s.Offset > baseLen || s.DeleteLen > baseLen-s.Offset {
			return fmt.Errorf("splice %d: range [%d,%d) exceeds base length %d", i, s.Offset, s.Offset+s.DeleteLen, baseLen)
		}
		if i > 0 && s.Offset < prevEnd {
			return fmt.Errorf("splice %d: offset %d overlaps previous splice ending at %d", i, s.Offset, prevEnd)
		}
		if !utf8.ValidString(s.Insert) {
			return fmt.Errorf("splice %d: insert is not valid UTF-8", i)
		}
		// Offsets must sit on rune boundaries of the base content.
		prevEnd = s.Offset + s.DeleteLen
	}
	return nil
}

// ApplyPatch applies parallel splices against content. Offsets are base
// coordinates; application walks splices in order, so each splice position
// is exact and untouched regions between splices are copied verbatim.
func ApplyPatch(content []byte, patch vmprotocol.TextPatch) ([]byte, error) {
	if err := ValidateSplices(patch.Splices, len(content)); err != nil {
		return nil, err
	}
	var out []byte
	base := 0
	for _, s := range patch.Splices {
		out = append(out, content[base:s.Offset]...)
		out = append(out, s.Insert...)
		base = s.Offset + s.DeleteLen
	}
	out = append(out, content[base:]...)
	return out, nil
}
