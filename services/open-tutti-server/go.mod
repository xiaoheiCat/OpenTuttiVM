module github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-server

go 1.24.3

require (
	github.com/coder/websocket v1.8.14
	github.com/hashicorp/yamux v0.1.2
	github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas v0.0.0
	github.com/xiaoheiCat/OpenTuttiVM/packages/agent/borrow v0.0.0
	github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol v0.0.0
	github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync v0.0.0
	golang.org/x/crypto v0.36.0
	modernc.org/sqlite v1.36.3
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/exp v0.0.0-20250305212735-054e65f0b394 // indirect
	golang.org/x/sys v0.41.0 // indirect
	modernc.org/libc v1.62.0 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace github.com/xiaoheiCat/OpenTuttiVM/packages/agent/borrow => ../../packages/agent/borrow

replace github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas => ../../packages/workspace/vm-cas

replace github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol => ../../packages/workspace/vm-protocol

replace github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync => ../../packages/workspace/vm-sync
