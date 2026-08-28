module github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-room-sync

go 1.24.3

toolchain go1.24.5

require github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-roomfs v0.0.0

require (
	github.com/coder/websocket v1.8.14
	github.com/hashicorp/yamux v0.1.2
	github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas v0.0.0
	github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol v0.0.0
	github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync v0.0.0
)

replace github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas => ../../packages/workspace/vm-cas

replace github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol => ../../packages/workspace/vm-protocol

replace github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync => ../../packages/workspace/vm-sync

replace github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-fs => ../open-tutti-fs

replace github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-roomfs => ../../packages/workspace/vm-roomfs
