package database

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestWorkspaceBatchRotatesGrantWithoutReleasingAdmission(t *testing.T) {
	client, session, taskID := workspaceStoreFixture(t)
	old, err := client.BeginWorkspaceAccess(t.Context(), session.Id, taskID, "result.glb", time.Minute)
	require.NoError(t, err)
	require.NoError(t, client.IssueWorkspaceRead(t.Context(), old))
	_, err = client.NextWorkspaceRead(t.Context(), old, "result.blend")
	require.ErrorIs(t, err, ErrConflict, "a still running read cannot advance")
	_, err = client.ConsumeWorkspaceGrant(t.Context(), session.Id, old.GrantID.String(), old.Path)
	require.NoError(t, err)
	require.NoError(t, client.CompleteWorkspaceRead(t.Context(), old))
	next, err := client.NextWorkspaceRead(t.Context(), old, "result.blend")
	require.NoError(t, err)
	require.NotEqual(t, old.GrantID, next.GrantID)
	require.Equal(t, old.ExecutorID, next.ExecutorID)
	require.Equal(t, old.TaskID, next.TaskID)
	require.Equal(t, old.ExpiresAt, next.ExpiresAt)
	require.Equal(t, "result.glb", old.Path, "the caller's old grant must not mutate")
	_, err = client.NextWorkspaceRead(t.Context(), old, "result.png")
	require.ErrorIs(t, err, ErrConflict)
	for _, pair := range []struct{ grant, path string }{{old.GrantID.String(), old.Path}, {next.GrantID.String(), old.Path}} {
		_, err := client.ConsumeWorkspaceGrant(t.Context(), session.Id, pair.grant, pair.path)
		require.ErrorIs(t, err, ErrNotFound)
	}
	_, err = client.ConsumeWorkspaceGrant(t.Context(), session.Id, next.GrantID.String(), next.Path)
	require.NoError(t, err)
	_, err = client.ConsumeWorkspaceGrant(t.Context(), session.Id, next.GrantID.String(), next.Path)
	require.ErrorIs(t, err, ErrNotFound)
	require.ErrorIs(t, client.ReserveSessionDispatch(t.Context(), session.Id, uuid.New(), "next"), ErrDispatchBusy)
}

func TestWorkspaceBatchAdvanceFencesStaleExecutorAndActor(t *testing.T) {
	for _, cause := range []string{"executor", "lease", "deadline", "actor", "recovery"} {
		t.Run(cause, func(t *testing.T) {
			client, session, taskID := workspaceStoreFixture(t)
			old, err := client.BeginWorkspaceAccess(t.Context(), session.Id, taskID, "result.glb", time.Minute)
			require.NoError(t, err)
			require.NoError(t, client.IssueWorkspaceRead(t.Context(), old))
			require.NoError(t, client.CompleteWorkspaceRead(t.Context(), old))
			switch cause {
			case "executor":
				old.ExecutorID = uuid.New()
			case "lease", "recovery":
				_, err = client.db.Exec(t.Context(), `UPDATE session_workspace_access SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE grant_id = $1`, old.GrantID)
			case "deadline":
				_, err = client.db.Exec(t.Context(), `UPDATE session_workspace_access SET expires_at = clock_timestamp() - interval '1 second' WHERE grant_id = $1`, old.GrantID)
			case "actor":
				_, err = client.db.Exec(t.Context(), `UPDATE session SET actor_uid = 'replaced' WHERE id = $1`, session.Id)
			}
			require.NoError(t, err)
			if cause == "recovery" {
				claimed, err := client.ClaimWorkspaceCleanup(t.Context())
				require.NoError(t, err)
				require.NotEqual(t, claimed.ExecutorID, old.ExecutorID)
			}
			next, err := client.NextWorkspaceRead(t.Context(), old, "result.blend")
			require.ErrorIs(t, err, ErrConflict)
			require.Nil(t, next)
			require.ErrorIs(t, client.ReserveSessionDispatch(t.Context(), session.Id, uuid.New(), "next"), ErrDispatchBusy)
		})
	}
}
