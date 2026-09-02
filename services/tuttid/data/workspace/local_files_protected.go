package workspace

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"

	workspacefiles "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/files"
)

var localFilesRuntimeGOOS = runtime.GOOS

var macOSProtectedRootDirectoryNames = map[string]struct{}{
	"applications": {},
	"bin":          {},
	"cores":        {},
	"dev":          {},
	"etc":          {},
	"library":      {},
	"network":      {},
	"opt":          {},
	"private":      {},
	"sbin":         {},
	"system":       {},
	"tmp":          {},
	"usr":          {},
	"var":          {},
	"volumes":      {},
}

var macOSProtectedHomeDirectoryNames = map[string]struct{}{
	"desktop":   {},
	"documents": {},
	"downloads": {},
	"movies":    {},
	"music":     {},
	"pictures":  {},
}

func isMacOSProtectedDirectory(root workspacefiles.WorkspaceRoot, logicalPath workspacefiles.LogicalPath) bool {
	if localFilesRuntimeGOOS != "darwin" {
		return false
	}

	physicalPath, err := workspacefiles.JoinPhysicalPath(root, logicalPath)
	if err != nil {
		return false
	}
	normalizedPath := cleanPhysicalDirectoryPath(physicalPath)
	if isDirectProtectedDirectory(normalizedPath, string(filepath.Separator), macOSProtectedRootDirectoryNames) {
		return true
	}

	homeDir, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(homeDir) == "" {
		return false
	}
	return isDirectProtectedDirectory(
		normalizedPath,
		cleanPhysicalDirectoryPath(homeDir),
		macOSProtectedHomeDirectoryNames,
	)
}

func isDirectProtectedDirectory(candidatePath string, parentPath string, protectedNames map[string]struct{}) bool {
	candidatePath = cleanPhysicalDirectoryPath(candidatePath)
	parentPath = cleanPhysicalDirectoryPath(parentPath)
	if candidatePath == parentPath || filepath.Dir(candidatePath) != parentPath {
		return false
	}
	_, protected := protectedNames[strings.ToLower(filepath.Base(candidatePath))]
	return protected
}

func cleanPhysicalDirectoryPath(value string) string {
	normalized := filepath.Clean(strings.TrimSpace(value))
	if normalized == "." || normalized == "" {
		return string(filepath.Separator)
	}
	return normalized
}
