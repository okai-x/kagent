package mcp

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	toolservice "github.com/kagent-dev/kagent/go/core/internal/service/tool"
	"github.com/kagent-dev/kagent/go/core/internal/version"
)

// ToolCatalog reads the registered tool metadata without making an authenticated
// HTTP request back to the controller. It never invokes a tool.
func (h *Handler) ToolCatalog(ctx context.Context) ([]toolservice.MCPAppTool, error) {
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := h.server.Connect(ctx, serverTransport, nil)
	if err != nil {
		return nil, fmt.Errorf("connect built-in MCP catalog server: %w", err)
	}
	defer serverSession.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "kagent-discovery", Version: version.Version}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		return nil, fmt.Errorf("connect built-in MCP catalog client: %w", err)
	}
	defer clientSession.Close()
	var tools []toolservice.MCPAppTool
	for tool, err := range clientSession.Tools(ctx, nil) {
		if err != nil {
			return nil, fmt.Errorf("list built-in MCP tools: %w", err)
		}
		tools = append(tools, toolservice.MCPAppTool{Name: tool.Name, Description: tool.Description})
	}
	return tools, nil
}
