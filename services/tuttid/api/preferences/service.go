package preferences

import (
	"context"

	preferencesbiz "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/biz/preferences"
	preferencesservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/preferences"
)

type Service interface {
	Get(context.Context) (preferencesbiz.DesktopPreferences, error)
	Put(context.Context, preferencesservice.PutInput) (preferencesbiz.DesktopPreferences, error)
}
