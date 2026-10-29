package mcp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/internal/service/sandbox"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

func TestToolCatalogMatchesRegisteredHTTPTools(t *testing.T) {
	handler, err := New(testSessionService(), testCheckpointService(), &fakeGateway{}, &sandbox.Service{}, nil)
	require.NoError(t, err)
	catalog, err := handler.ToolCatalog(t.Context())
	require.NoError(t, err)
	names := make(map[string]string)
	for _, tool := range catalog {
		names[tool.Name] = tool.Description
	}
	for _, name := range []string{listToolName, invokeToolName, createCheckpointToolName, "start_sandbox_process", "read_sandbox_file", "write_sandbox_file"} {
		require.Contains(t, names, name)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	response := rawMCPCall(t, server.URL, "tools/list", map[string]any{}, false)
	httpTools := response["result"].(map[string]any)["tools"].([]any)
	require.Len(t, catalog, len(httpTools))
	for _, value := range httpTools {
		tool := value.(map[string]any)
		require.Equal(t, tool["description"], names[tool["name"].(string)])
	}
}

func TestToolCatalogDoesNotAllowAnonymousHTTP(t *testing.T) {
	handler, err := New(testSessionService(), testCheckpointService(), &fakeGateway{}, nil, nil)
	require.NoError(t, err)
	catalog, err := handler.ToolCatalog(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, catalog)
	protected := auth.AuthnMiddleware(&authimpl.RejectUserAuthenticator{})(handler)
	for _, test := range []struct {
		method        string
		authorization string
	}{
		{method: "initialize"},
		{method: "tools/list"},
		{method: "tools/call"},
		{method: "initialize", authorization: "Bearer kagent-credential-injected"},
	} {
		t.Run(test.method+test.authorization, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"`+test.method+`","params":{}}`))
			if test.authorization != "" {
				request.Header.Set("Authorization", test.authorization)
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json, text/event-stream")
			response := httptest.NewRecorder()
			protected.ServeHTTP(response, request)
			require.Equal(t, http.StatusUnauthorized, response.Code)
		})
	}
}
