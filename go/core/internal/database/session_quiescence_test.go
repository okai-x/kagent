package database

import (
	"context"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

func quiescenceStoreFixture(t *testing.T) (*Client, *apiv1.Session) {
	t.Helper()
	client := NewClient(setupTestDB(t), "public")
	sessionFixture(t, client, t.Context(), "team-a", "revision", "assistant", "kagent")
	session, _, err := client.CreateSession(t.Context(), newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), uuid.NewString())
	require.NoError(t, err)
	session, err = markSessionReady(t.Context(), client, session.Id, "agent.example")
	require.NoError(t, err)
	task := newSessionTask(uuid.NewString(), "initial")
	task.ContextID, task.Status.State = session.ContextId, a2a.TaskStateCompleted
	version, err := client.CreateRuntimeTask(t.Context(), session.Id, taskMutationHash("initial"), task, "")
	require.NoError(t, err)
	require.NoError(t, client.SettleSessionTask(t.Context(), session.Id, string(task.ID), version))
	return client, session
}

func TestQuiescencePreparationCrashAndStaleExecutor(t *testing.T) {
	client, session := quiescenceStoreFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	abandoned, err := client.ClaimSessionQuiescence(ctx)
	require.NoError(t, err)
	restarted := NewClient(client.db, "public")
	var current *SessionQuiescence
	require.Eventually(t, func() bool {
		current, err = restarted.ClaimSessionQuiescence(t.Context())
		return err == nil
	}, time.Second, 10*time.Millisecond, "read-only preparation must recover after a worker crash")
	require.NotEqual(t, abandoned.ExecutorID, current.ExecutorID)
	require.ErrorIs(t, client.IssueSessionQuiescence(t.Context(), abandoned, "actor-"+session.Id, 7), ErrConflict)
	require.NoError(t, client.ReleaseSessionQuiescence(t.Context(), abandoned))
	require.ErrorIs(t, client.IssueSessionQuiescence(t.Context(), current, "replacement-actor", 7), ErrConflict)
	require.NoError(t, restarted.IssueSessionQuiescence(t.Context(), current, "actor-"+session.Id, 7))
	require.ErrorIs(t, restarted.IssueSessionQuiescence(t.Context(), current, "actor-"+session.Id, 7), ErrConflict,
		"the same executor must not receive permission to issue twice")
	require.NoError(t, client.ReleaseSessionQuiescence(t.Context(), current))
	_, err = restarted.ClaimSessionQuiescence(t.Context())
	require.ErrorIs(t, err, ErrNotFound, "issued work never becomes retryable")
	pending, err := client.HasPendingSessionQuiescence(t.Context())
	require.NoError(t, err)
	require.False(t, pending)
	observed, err := queryOne(t.Context(), client.db, `
		SELECT quiescence_actor_uid AS uid, quiescence_snapshot_generation AS generation
		FROM session_task_event WHERE sequence = $1
	`, pgx.RowToStructByName[struct {
		UID        string
		Generation int32
	}], current.Version)
	require.NoError(t, err)
	require.Equal(t, "actor-"+session.Id, observed.UID)
	require.EqualValues(t, 7, observed.Generation)
	snapshot := &SessionTaskSnapshot{Atespace: "team-a", URI: "s3://snapshot", Fidelity: "SNAPSHOT_FIDELITY_VOLUMES"}
	require.ErrorIs(t, client.FinishSessionQuiescence(t.Context(), abandoned, snapshot), ErrNotFound)
	require.NoError(t, restarted.FinishSessionQuiescence(t.Context(), current, snapshot))
	require.NoError(t, restarted.FinishSessionQuiescence(t.Context(), current, snapshot))
}

func TestQuiescencePreparationFailureAllowsNewTurn(t *testing.T) {
	client, session := quiescenceStoreFixture(t)
	work, err := client.ClaimSessionQuiescence(t.Context())
	require.NoError(t, err)
	require.NoError(t, client.ReleaseSessionQuiescence(t.Context(), work))
	next := newSessionTask(uuid.NewString(), "next")
	next.ContextID = session.ContextId
	_, err = client.CreateRuntimeTask(t.Context(), session.Id, taskMutationHash("next"), next, "")
	require.NoError(t, err, "no runtime work was issued, so admission can safely supersede preparation")
	require.ErrorIs(t, client.IssueSessionQuiescence(t.Context(), work, "actor-"+session.Id, 1), ErrConflict)
	_, err = client.ClaimSessionQuiescence(t.Context())
	require.ErrorIs(t, err, ErrNotFound)
}
