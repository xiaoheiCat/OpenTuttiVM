package reporter

import (
	analyticsreporter "github.com/xiaoheiCat/OpenTuttiVM/packages/analytics/reporter-go"
	tuttitypes "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/types"
)

type Event = analyticsreporter.Event
type DebugEvent = analyticsreporter.DebugEvent
type DebugPublisher = analyticsreporter.DebugPublisher
type Reporter = analyticsreporter.Reporter
type NoopReporter = analyticsreporter.NoopReporter
type DebugReporter = analyticsreporter.DebugReporter
type TeaReporter = analyticsreporter.TeaReporter
type DynamicContext = analyticsreporter.DynamicContext

type Config struct {
	Analytics              tuttitypes.AnalyticsConfig
	DebugPublisher         DebugPublisher
	StateDir               string
	CommonParams           map[string]any
	DynamicContextProvider func() DynamicContext
}

func New(config Config) (Reporter, error) {
	return analyticsreporter.New(analyticsreporter.Config{
		Analytics: analyticsreporter.AnalyticsConfig{
			Disabled:      config.Analytics.Disabled,
			Debug:         config.Analytics.Debug,
			AppID:         int64(config.Analytics.AppID),
			AppKey:        config.Analytics.AppKey,
			Channel:       config.Analytics.Channel,
			ChannelDomain: config.Analytics.ChannelDomain,
			AppVersion:    config.Analytics.AppVersion,
		},
		DebugPublisher:         config.DebugPublisher,
		StateDir:               config.StateDir,
		CommonParams:           config.CommonParams,
		DynamicContextProvider: config.DynamicContextProvider,
	})
}
