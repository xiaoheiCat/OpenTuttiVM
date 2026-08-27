package main

import "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/daemon/providerregistry"

func tuttiDesktopCommandNetworkAccessPolicy(provider string) bool {
	descriptor, ok := providerregistry.Find(provider)
	return ok && descriptor.Desktop.CommandNetworkAccess
}
