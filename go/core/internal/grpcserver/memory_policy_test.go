package grpcserver

import (
	"context"
	"testing"

	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	runtimev1 "github.com/kagent-dev/kagent/go/api/gen/kagent/runtime/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/internal/jwttest"
	"github.com/kagent-dev/kagent/go/core/internal/runtimeauth"
	memorysvc "github.com/kagent-dev/kagent/go/core/internal/service/memory"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

func TestActorMemoryPinnedPolicyThroughGRPCAndPostgres(t *testing.T) {
	dsn := dbtest.StartT(context.WithoutCancel(t.Context()), t)
	dbtest.MigrateT(t, dsn, true)
	pool, err := database.Connect(t.Context(), &database.PostgresConfig{Pool: dbtest.PoolConfig(dsn)})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	store := database.NewClient(pool, "public")
	issuer := jwttest.New(t)
	verifier, err := substrate.NewActorJWTVerifier(t.Context(), issuer.URL, issuer.CAFile, substrate.RuntimeJWTAudience)
	require.NoError(t, err)
	connection := newConfigurationConnection(t, nil, func(config *Config) {
		config.Authenticator = runtimeauth.NewProvider(&authimpl.RejectUserAuthenticator{}, verifier, store, issuer.URL)
		config.MemoryService = memorysvc.NewService(store, runtimeauth.NewAuthorizer(auth.NoopAuthorizer{}, store))
	})
	client := runtimev1.NewMemoryServiceClient(connection)
	calls := actorMemoryCalls(client, "team-a/assistant")
	var disabledContexts []context.Context
	var enabledContexts []context.Context
	var enabledRevision string
	for _, enabled := range []bool{false, true} {
		revision := database.RuntimeRevision{
			Revision: uuid.NewString(), Namespace: "team-a", AgentName: "assistant", AgentUID: "agent-uid",
			SourceSnapshot: []byte("{}"), AgentCard: &a2apb.AgentCard{Name: "assistant"}, MemoryEnabled: enabled,
			ActorTemplateAtespace: "team-a", ActorTemplateName: uuid.NewString(), ActorTemplateUID: uuid.NewString(),
		}
		require.NoError(t, store.UpsertAgentDefinition(t.Context(), database.AgentDefinition{
			Namespace: revision.Namespace, AgentName: revision.AgentName, AgentUID: revision.AgentUID, DesiredRevision: revision.Revision,
		}))
		require.NoError(t, store.RecordRuntimeRevision(t.Context(), revision, database.RecordRevisionOptions{Ready: true}))
		for range 2 {
			session := createMemoryPolicySession(t, store)
			require.Equal(t, revision.Revision, session.PreparedRevision)
			bound, err := store.HasSessionSandboxBinding(t.Context(), session.Id)
			require.NoError(t, err)
			require.False(t, bound, "exercise single-Actor Sessions without a Sandbox")
			token := issuer.Token(t, issuer.Claims("team-a", substrate.ActorName(session.Id), "actor-uid"))
			ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+token))
			if !enabled {
				disabledContexts = append(disabledContexts, ctx)
				continue
			}
			enabledContexts = append(enabledContexts, ctx)
			enabledRevision = revision.Revision
			for _, call := range calls {
				require.NoError(t, call.call(ctx), call.name)
			}
		}
	}
	// Keep a memory in the shared Agent/owner partition. Neither old Session
	// may read, mutate, or erase it after a new revision enables Memory.
	require.NoError(t, store.StoreAgentMemories(t.Context(), &database.Memory{
		AgentName: "team-a/assistant", UserID: "shared-product-owner", Content: "retained", Metadata: "{}",
		Embedding: pgvector.NewVector(memoryVector()),
	}))
	for _, ctx := range disabledContexts {
		for _, call := range calls {
			require.Equal(t, codes.PermissionDenied, status.Code(call.call(ctx)), call.name)
		}
	}
	rows, err := store.ListAgentMemories(t.Context(), "team-a/assistant", "shared-product-owner")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "retained", rows[0].Content)
	// Malformed durable records must not fall back to the shared partition.
	_, err = pool.Exec(t.Context(), `UPDATE agent_revision SET agent_card = $2 WHERE revision = $1`, enabledRevision, []byte{0xff})
	require.NoError(t, err)
	for _, ctx := range enabledContexts {
		for _, call := range calls {
			require.Equal(t, codes.Internal, status.Code(call.call(ctx)), call.name)
		}
	}
	rows, err = store.ListAgentMemories(t.Context(), "team-a/assistant", "shared-product-owner")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "retained", rows[0].Content)
}

func createMemoryPolicySession(t *testing.T, store *database.Client) *apiv1.Session {
	t.Helper()
	session, _, err := store.CreateSession(t.Context(), &apiv1.Session{
		Id: uuid.NewString(), Creator: "shared-product-owner", Agent: &apiv1.ResourceReference{Namespace: "team-a", Name: "assistant"},
	}, uuid.NewString())
	require.NoError(t, err)
	operation, err := store.BeginSessionOperation(t.Context(), session.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := store.ClaimSessionOperation(t.Context(), session.Id, operation.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	session, err = store.FinishSessionOperation(t.Context(), session.Id, operation.ID, executor,
		substrate.ActorHost("team-a", substrate.ActorName(session.Id), ""), "actor-uid", "")
	require.NoError(t, err)
	return session
}
