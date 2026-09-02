package agentsessionreplay

import replay "github.com/xiaoheiCat/OpenTuttiVM/packages/agent/session-replay"

var ErrCassetteAlreadyExists = replay.ErrCassetteAlreadyExists

type Store = replay.Store
type SemanticCassetteReader = replay.SemanticCassetteReader

var NewSemanticCassetteReader = replay.NewSemanticCassetteReader
