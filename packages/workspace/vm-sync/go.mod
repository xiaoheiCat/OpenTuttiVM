module github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-sync

go 1.24.3

toolchain go1.24.5

require github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas v0.0.0

require github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol v0.0.0

replace github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-cas => ../vm-cas

replace github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol => ../vm-protocol
