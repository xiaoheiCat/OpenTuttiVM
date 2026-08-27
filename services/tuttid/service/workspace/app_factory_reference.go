package workspace

import (
	"embed"
	"fmt"
	"io/fs"
	"path/filepath"

	agentservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/agent"
)

//go:embed app_factory_reference/*
var appFactoryReferenceFiles embed.FS

const appFactoryReferenceRoot = "app_factory_reference"

func appFactoryReferenceSkillBundle() (agentservice.SessionSkillBundle, error) {
	files := make(map[string]string)
	if err := fs.WalkDir(appFactoryReferenceFiles, appFactoryReferenceRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relativePath, err := filepath.Rel(appFactoryReferenceRoot, path)
		if err != nil {
			return fmt.Errorf("resolve app factory skill path: %w", err)
		}
		data, err := appFactoryReferenceFiles.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read app factory skill file: %w", err)
		}
		files[filepath.ToSlash(relativePath)] = string(data)
		return nil
	}); err != nil {
		return agentservice.SessionSkillBundle{}, err
	}
	return agentservice.SessionSkillBundle{
		Name:  "app-factory",
		Files: files,
	}, nil
}
