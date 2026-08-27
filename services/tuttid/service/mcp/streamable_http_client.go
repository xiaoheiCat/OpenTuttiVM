// Package mcp preserves the tuttId-local import during the public MCP client
// extraction. New consumers should import packages/connector/runtime/mcp.
package mcp

import runtimemcp "github.com/xiaoheiCat/OpenTuttiVM/packages/connector/runtime/mcp"

type RequestAuthorizer = runtimemcp.RequestAuthorizer
type StreamableHTTPClientConfig = runtimemcp.StreamableHTTPClientConfig
type StreamableHTTPClient = runtimemcp.StreamableHTTPClient

var NewStreamableHTTPClient = runtimemcp.NewStreamableHTTPClient
