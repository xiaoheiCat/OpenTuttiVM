package agent

import (
	"context"
	"sync"
	"time"

	agenthost "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/host"
	claudecodeservice "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/service/claudecode"
)

type serviceHostLocker struct {
	mu    *sync.Mutex
	locks *map[string]*serviceSessionSettingsLock
}

type serviceHeldSessionLock struct {
	owner any
	ref   agenthost.SessionRef
}

type serviceHeldSessionLockContextKey struct{}

func withServiceHeldSessionLock(ctx context.Context, service *Service, ref agenthost.SessionRef) context.Context {
	return context.WithValue(ctx, serviceHeldSessionLockContextKey{}, serviceHeldSessionLock{owner: service.sessionSettingsLockIdentity(), ref: ref})
}

func (a serviceHostLocker) Acquire(ctx context.Context, ref agenthost.SessionRef) (func(), error) {
	if held, ok := ctx.Value(serviceHeldSessionLockContextKey{}).(serviceHeldSessionLock); ok && held.owner == a.locks && held.ref == ref {
		return func() {}, nil
	}
	return acquireServiceSessionSettingsLock(ctx, a.mu, a.locks, ref.WorkspaceID, ref.AgentSessionID)
}

type serviceHostStartupGate struct {
	gate *claudecodeservice.StartupGate
}

func (a serviceHostStartupGate) Acquire(ctx context.Context, provider string) (func(), error) {
	if !isClaudeSDKLiveModelProvider(provider) {
		return func() {}, nil
	}
	gate := a.gate
	if gate == nil {
		gate = claudecodeservice.DefaultStartupGate
	}
	if err := gate.Acquire(ctx); err != nil {
		return nil, err
	}
	return gate.Release, nil
}

type serviceHostClock struct {
	now func() time.Time
}

func (c serviceHostClock) Now() time.Time {
	if c.now != nil {
		return c.now().UTC()
	}
	return time.Now().UTC()
}
