package database

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

func TestSandboxSessionBindingOwnershipAndPagination(t *testing.T) {
	store := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	sessionFixture(t, store, ctx, "team-a", "agent-revision", "assistant", "kagent")
	first, _, err := store.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), "first")
	require.NoError(t, err)
	second, _, err := store.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), "second")
	require.NoError(t, err)
	input, options := sandboxFixture(t, store, "sandbox-revision")
	for _, sessionID := range []string{"", first.Id, second.Id} {
		request := proto.CloneOf(input)
		request.Id, request.SessionId = uuid.NewString(), sessionID
		created, _, err := store.CreateSandbox(ctx, request, uuid.NewString(), options)
		require.NoError(t, err)
		require.Equal(t, sessionID, created.SessionId)
		read, err := store.GetSandbox(ctx, created.Id, input.Creator)
		require.NoError(t, err)
		require.Equal(t, sessionID, read.SessionId)
	}
	listed, err := store.ListSandboxes(ctx, input.Creator, "", 1, nil, first.Id)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, first.Id, listed[0].SessionId)
	for _, sessionID := range []string{first.Id, uuid.NewString()} {
		request := proto.CloneOf(input)
		request.Id, request.Creator, request.SessionId = uuid.NewString(), "other-owner", sessionID
		_, _, err := store.CreateSandbox(ctx, request, uuid.NewString(), options)
		require.ErrorIs(t, err, ErrNotFound)
	}
}

func TestBoundSandboxRetainsExpiredSessionTombstone(t *testing.T) {
	store, session := expirationFixture(t)
	ctx := t.Context()
	input, options := sandboxFixture(t, store, "bound-expiration")
	input.SessionId = session.Id
	created, _, err := store.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	_, err = store.BeginIdleSessionDeletion(ctx, session.Id, time.Now())
	require.NoError(t, err)
	deleted, err := finishSessionOperation(ctx, store, session.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE, "")
	require.NoError(t, err)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_DELETED, deleted.State)
	_, err = store.GetSessionByID(ctx, session.Id)
	require.ErrorIs(t, err, ErrNotFound, "tombstones are not visible as active Sessions")
	instance, err := store.GetSandbox(ctx, created.Id, created.Creator)
	require.NoError(t, err)
	require.Equal(t, session.Id, instance.SessionId)
	_, fresh, err := store.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), "conversation")
	require.ErrorIs(t, err, ErrFailedPrecondition)
	require.False(t, fresh, "the original creation receipt cannot rebind an existing Sandbox")
}

func TestSandboxBindingSurvivesSandboxDeletion(t *testing.T) {
	store, session := expirationFixture(t)
	ctx := t.Context()
	bound, err := store.HasSessionSandboxBinding(ctx, session.Id)
	require.NoError(t, err)
	require.False(t, bound)
	input, options := sandboxFixture(t, store, "bound-memory")
	input.SessionId = session.Id
	created, _, err := store.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	finishSandbox(t, store, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	for _, deleted := range []bool{false, true} {
		if deleted {
			finishSandbox(t, store, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
		}
		bound, err = store.HasSessionSandboxBinding(ctx, session.Id)
		require.NoError(t, err)
		require.True(t, bound, "deletion cannot restore shared memory access")
	}
}
