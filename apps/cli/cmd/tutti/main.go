package main

import (
	"context"
	"os"

	"github.com/xiaoheiCat/OpenTuttiVM/apps/cli/internal/app"
)

func main() {
	os.Exit(app.RunWithProgram(context.Background(), os.Args[0], os.Args[1:], os.Stdout, os.Stderr))
}
