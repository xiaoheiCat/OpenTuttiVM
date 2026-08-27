package agent

import (
	agenthost "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/host"
	storesqlite "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/store-sqlite"
)

// ApplicationHost returns the Host installed during service construction.
// Missing wiring is a startup invariant violation; this adapter never creates
// a second lifecycle stack from service-local store/runtime copies.
func (s *Service) ApplicationHost() *agenthost.Host {
	if s == nil {
		return nil
	}
	s.applicationHostMu.Lock()
	provider := s.applicationHostProvider
	s.applicationHostMu.Unlock()
	if provider == nil {
		panic("agent service application host is not configured")
	}
	host := provider()
	if host == nil {
		panic("agent service application host provider returned nil")
	}
	return host
}

func persistedSessionFromHost(session storesqlite.Session) PersistedSession {
	return persistedSessionFromActivity(session)
}
