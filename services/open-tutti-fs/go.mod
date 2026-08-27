module github.com/xiaoheiCat/OpenTuttiVM/services/open-tutti-fs

go 1.24.3

toolchain go1.24.5

require github.com/hanwen/go-fuse/v2 v2.11.0

require golang.org/x/sys v0.28.0 // indirect

replace github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas => ../../packages/workspace/vm-cas

replace github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol => ../../packages/workspace/vm-protocol

replace github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync => ../../packages/workspace/vm-sync
