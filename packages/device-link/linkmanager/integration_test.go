package linkmanager_test

import (
	"github.com/xiaoheiCat/OpenTuttiVM/packages/device-link/authenticated"
	"github.com/xiaoheiCat/OpenTuttiVM/packages/device-link/linkmanager"
)

var _ linkmanager.Link = (*authenticated.Link)(nil)
