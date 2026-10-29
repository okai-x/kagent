package grpcserver

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	runtimev1 "github.com/kagent-dev/kagent/go/api/gen/kagent/runtime/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/internal/jwttest"
	"github.com/kagent-dev/kagent/go/core/internal/runtimeauth"
	memorysvc "github.com/kagent-dev/kagent/go/core/internal/service/memory"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

func TestSandboxBoundActorsCannotUseSharedMemory(t *testing.T) {
	issuer := jwttest.New(t)
	verifier, err := substrate.NewActorJWTVerifier(t.Context(), issuer.URL, issuer.CAFile, substrate.RuntimeJWTAudience)
	require.NoError(t, err)
	store := &memoryRPCStore{memoryEnabled: true, rows: map[string][]database.Memory{}, sessions: map[string]*apiv1.Session{}, boundSessions: map[string]bool{}}
	var contexts []context.Context
	for _, bound := range []bool{true, true, false} {
		id := uuid.NewString()
		store.sessions[id] = &apiv1.Session{Id: id, Creator: "shared-product-owner", PreparedRevision: "revision",
			Agent:        &apiv1.ResourceReference{Namespace: "team", Name: "assistant"},
			A2AAuthority: substrate.ActorHost("team", substrate.ActorName(id), "")}
		store.boundSessions[id] = bound
		token := issuer.Token(t, issuer.Claims("team", substrate.ActorName(id), "actor-uid"))
		contexts = append(contexts, metadata.NewOutgoingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+token)))
	}
	connection := newConfigurationConnection(t, nil, func(config *Config) {
		config.Authenticator = runtimeauth.NewProvider(&authimpl.RejectUserAuthenticator{}, verifier, store, issuer.URL)
		config.MemoryService = memorysvc.NewService(store, runtimeauth.NewAuthorizer(auth.NoopAuthorizer{}, store))
	})
	client := runtimev1.NewMemoryServiceClient(connection)
	calls := actorMemoryCalls(client, "team/assistant")
	for _, ctx := range contexts[:2] {
		for _, call := range calls {
			require.Equal(t, codes.PermissionDenied, status.Code(call.call(ctx)), call.name)
		}
	}
	require.Zero(t, store.callCount(), "bound actors never reach the shared memory partition")
	// A failed binding lookup must not select the legacy owner partition.
	store.mu.Lock()
	store.bindingError = errors.New("lookup unavailable")
	store.mu.Unlock()
	for _, call := range calls {
		require.Equal(t, codes.Internal, status.Code(call.call(contexts[2])), call.name)
	}
	require.Zero(t, store.callCount())
	store.mu.Lock()
	store.bindingError = nil
	store.mu.Unlock()
	for _, call := range calls {
		require.NoError(t, call.call(contexts[2]), "unbound Memory-enabled session: %s", call.name)
	}
	require.Equal(t, len(calls), store.callCount())
}

type memoryRPCCall struct {
	name string
	call func(context.Context) error
}

func actorMemoryCalls(client runtimev1.MemoryServiceClient, agent string) []memoryRPCCall {
	input := &runtimev1.SessionMemoryInput{Content: "private", Vector: memoryVector()}
	return []memoryRPCCall{
		{"add", func(ctx context.Context) error {
			_, err := client.AddSession(ctx, &runtimev1.MemoryServiceAddSessionRequest{Memory: input})
			return err
		}},
		{"batch", func(ctx context.Context) error {
			_, err := client.AddSessionBatch(ctx, &runtimev1.MemoryServiceAddSessionBatchRequest{Items: []*runtimev1.SessionMemoryInput{input}})
			return err
		}},
		{"search", func(ctx context.Context) error {
			_, err := client.Search(ctx, &runtimev1.MemoryServiceSearchRequest{Vector: memoryVector()})
			return err
		}},
		{"list", func(ctx context.Context) error {
			_, err := client.List(ctx, &runtimev1.MemoryServiceListRequest{AgentName: agent})
			return err
		}},
		{"delete", func(ctx context.Context) error {
			_, err := client.Delete(ctx, &runtimev1.MemoryServiceDeleteRequest{AgentName: agent})
			return err
		}},
	}
}
