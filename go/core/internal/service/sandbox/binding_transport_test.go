package sandbox_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/google/uuid"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/jwttest"
	toolserver "github.com/kagent-dev/kagent/go/core/internal/mcp"
	"github.com/kagent-dev/kagent/go/core/internal/runtimeauth"
	"github.com/kagent-dev/kagent/go/core/internal/service/checkpoint"
	"github.com/kagent-dev/kagent/go/core/internal/service/session"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

type actorMCPTransport struct{ token string }

func (t actorMCPTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+t.token)
	r.Header.Set("X-User-Id", "forged-owner")
	r.Header.Set("X-Agent-Name", "other/agent")
	return http.DefaultTransport.RoundTrip(r)
}

func boundSession(t *testing.T, store *database.Client) *apiv1.Session {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, store.UpsertAgentDefinition(ctx, database.AgentDefinition{
		Namespace: "team-a", AgentName: "assistant", AgentUID: "agent-uid", DesiredRevision: "agent-revision",
	}))
	require.NoError(t, store.RecordRuntimeRevision(ctx, database.RuntimeRevision{
		Revision: "agent-revision", Namespace: "team-a", AgentName: "assistant", AgentUID: "agent-uid",
		SourceSnapshot: []byte("{}"), AgentCard: &a2apb.AgentCard{}, ActorTemplateAtespace: "team-a", ActorTemplateName: "agent", ActorTemplateUID: "template-uid",
	}, database.RecordRevisionOptions{Ready: true}))
	created, _, err := store.CreateSession(ctx, &apiv1.Session{Id: uuid.NewString(), Creator: "alice",
		Agent: &apiv1.ResourceReference{Namespace: "team-a", Name: "assistant"}}, uuid.NewString())
	require.NoError(t, err)
	operation, err := store.BeginSessionOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := store.ClaimSessionOperation(ctx, created.Id, operation.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	created, err = store.FinishSessionOperation(ctx, created.Id, operation.ID, executor,
		substrate.ActorHost("team-a", substrate.ActorName(created.Id), ""), "actor-"+created.Id, "")
	require.NoError(t, err)
	return created
}

func TestSandboxActorMCPUsesSignedSessionBinding(t *testing.T) {
	issuer := jwttest.New(t)
	verifier, err := substrate.NewActorJWTVerifier(t.Context(), issuer.URL, issuer.CAFile, substrate.RuntimeJWTAudience)
	require.NoError(t, err)
	var provider *runtimeauth.Provider
	service, owner, actors, headers, store := configuredGuestFixture(t, func(store *database.Client) (auth.AuthProvider, auth.Authorizer) {
		provider = runtimeauth.NewProvider(testAuth{}, verifier, store, issuer.URL)
		return provider, runtimeauth.NewAuthorizer(auth.NoopAuthorizer{}, store)
	})
	first, second := boundSession(t, store), boundSession(t, store)
	var sandboxes []*apiv1.Sandbox
	for _, sessionID := range []string{first.Id, second.Id, ""} {
		actors.mu.Lock()
		actors.actor = nil
		actors.mu.Unlock()
		request := createRequest()
		request.RequestId, request.SessionId = uuid.NewString(), sessionID
		created, err := service.Create(owner, request)
		require.NoError(t, err)
		sandboxes = append(sandboxes, created)
	}
	handler, err := toolserver.New(&session.Service{}, &checkpoint.Service{}, &a2asrv.InterceptedHandler{}, service, nil)
	require.NoError(t, err)
	server := httptest.NewServer(auth.AuthnMiddleware(provider)(handler))
	t.Cleanup(server.Close)
	token := issuer.Token(t, issuer.Claims("team-a", substrate.ActorName(first.Id), "actor-"+first.Id))
	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "binding-test", Version: "test"}, nil)
	connection, err := client.Connect(t.Context(), &sdkmcp.StreamableClientTransport{
		Endpoint: server.URL, HTTPClient: &http.Client{Transport: actorMCPTransport{token}},
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	call := func(name string, arguments map[string]any, allowed bool) *sdkmcp.CallToolResult {
		t.Helper()
		result, err := connection.CallTool(t.Context(), &sdkmcp.CallToolParams{Name: name, Arguments: arguments})
		require.NoError(t, err)
		require.Equal(t, !allowed, result.IsError, "%s: %+v", name, result.Content)
		return result
	}
	for index, sandbox := range sandboxes {
		allowed := index == 0
		call("get_sandbox", map[string]any{"sandbox_id": sandbox.Id}, allowed)
		call("write_sandbox_file", map[string]any{"sandbox_id": sandbox.Id, "path": "bound.txt", "data_base64": "c2NvcGVk"}, allowed)
		call("read_sandbox_file", map[string]any{"sandbox_id": sandbox.Id, "path": "bound.txt"}, allowed)
		call("start_sandbox_process", map[string]any{"sandbox_id": sandbox.Id, "command": []string{"true"}}, allowed)
		for _, name := range []string{"delete_sandbox", "suspend_sandbox", "resume_sandbox"} {
			call(name, map[string]any{"sandbox_id": sandbox.Id}, false)
		}
	}
	require.Equal(t, "team-a/"+substrate.ActorName(sandboxes[0].Id), headers().Get("ate-target-actor"))
	require.Equal(t, "alice", headers().Get("x-user-id"))
	require.Empty(t, headers().Get("Authorization"), "Actor JWT never becomes a product credential")
	call("create_sandbox", map[string]any{"namespace": "team-a", "template": "scratch", "request_id": uuid.NewString()}, false)
	listed := call("list_sandboxes", map[string]any{"page_size": 1}, true)
	var page struct {
		Sandboxes []struct {
			ID string `json:"id"`
		} `json:"sandboxes"`
		Next string `json:"next_page_token"`
	}
	require.NoError(t, json.Unmarshal([]byte(listed.Content[0].(*sdkmcp.TextContent).Text), &page))
	require.Len(t, page.Sandboxes, 1)
	require.Equal(t, sandboxes[0].Id, page.Sandboxes[0].ID)
	require.Empty(t, page.Next)
	// Product ownership remains authoritative, while forged headers cannot change Actor scope.
	_, err = service.Get(owner, sandboxes[1].Id)
	require.NoError(t, err)
}
