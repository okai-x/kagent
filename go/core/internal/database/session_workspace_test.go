package database

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

func workspaceStoreFixture(t *testing.T) (*Client, *apiv1.Session, string) {
	t.Helper()
	client, session := quiescenceStoreFixture(t)
	tasks, _, err := client.ListSessionTasks(t.Context(), session.Id, "", "", nil, 10, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	return client, session, string(tasks[0].ID)
}

func TestWorkspaceAccessAdmissionAndSingleUseGrant(t *testing.T) {
	client, session, taskID := workspaceStoreFixture(t)
	_, err := client.BeginWorkspaceAccess(t.Context(), session.Id, "old-head", "result.glb", time.Minute)
	require.ErrorIs(t, err, ErrFailedPrecondition)
	work, err := client.BeginWorkspaceAccess(t.Context(), session.Id, taskID, "result.glb", time.Minute)
	require.NoError(t, err)
	_, err = client.BeginWorkspaceAccess(t.Context(), session.Id, taskID, "result.glb", time.Minute)
	require.ErrorIs(t, err, ErrFailedPrecondition)
	require.ErrorIs(t, client.ReserveSessionDispatch(t.Context(), session.Id, uuid.New(), "next"), ErrDispatchBusy)
	_, err = client.BeginSessionOperation(t.Context(), session.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.ErrorIs(t, err, ErrFailedPrecondition)
	_, _, err = client.ReserveSessionCheckpoint(t.Context(), &apiv1.Checkpoint{Id: uuid.NewString(), SessionId: session.Id, HeadTaskId: taskID}, session.Creator, uuid.NewString())
	require.ErrorIs(t, err, ErrSnapshotPending)
	_, err = client.ClaimSessionQuiescence(t.Context())
	require.ErrorIs(t, err, ErrNotFound)
	next := newSessionTask(uuid.NewString(), "next")
	next.ContextID = session.ContextId
	_, err = client.CreateRuntimeTask(t.Context(), session.Id, taskMutationHash("next"), next, "")
	require.ErrorIs(t, err, ErrFailedPrecondition)
	_, err = client.ConsumeWorkspaceGrant(t.Context(), session.Id, work.GrantID.String(), work.Path)
	require.ErrorIs(t, err, ErrNotFound, "preparation does not authorize runtime access")
	require.NoError(t, client.IssueWorkspaceRead(t.Context(), work))
	require.ErrorIs(t, client.IssueWorkspaceRead(t.Context(), work), ErrConflict)
	for _, input := range []struct{ sessionID, path string }{{uuid.NewString(), work.Path}, {session.Id, "other.glb"}} {
		_, err := client.ConsumeWorkspaceGrant(t.Context(), input.sessionID, work.GrantID.String(), input.path)
		require.ErrorIs(t, err, ErrNotFound)
	}
	expires, err := client.ConsumeWorkspaceGrant(t.Context(), session.Id, work.GrantID.String(), work.Path)
	require.NoError(t, err)
	require.Equal(t, work.ExpiresAt, expires)
	_, err = client.ConsumeWorkspaceGrant(t.Context(), session.Id, work.GrantID.String(), work.Path)
	require.ErrorIs(t, err, ErrNotFound, "one grant must not create two private streams")
	require.NoError(t, client.CompleteWorkspaceRead(t.Context(), work))
	require.NoError(t, client.IssueWorkspaceQuiescence(t.Context(), work, 1))
	require.ErrorIs(t, client.IssueWorkspaceQuiescence(t.Context(), work, 1), ErrConflict)
	snapshot := &SessionTaskSnapshot{Atespace: "team-a", URI: "s3://workspace-snapshot", Fidelity: "SNAPSHOT_FIDELITY_VOLUMES"}
	require.NoError(t, client.FinishWorkspaceAccess(t.Context(), work, snapshot))
	require.NoError(t, client.FinishWorkspaceAccess(t.Context(), work, snapshot), "lost completion response is idempotent")
	_, err = client.ConsumeWorkspaceGrant(t.Context(), session.Id, work.GrantID.String(), work.Path)
	require.ErrorIs(t, err, ErrNotFound)
	checkpoint, retained, err := client.ReserveSessionCheckpoint(t.Context(), &apiv1.Checkpoint{Id: uuid.NewString(), SessionId: session.Id, HeadTaskId: taskID}, session.Creator, uuid.NewString())
	require.NoError(t, err)
	require.NotNil(t, checkpoint)
	require.Equal(t, snapshot, retained)
}

func TestWorkspacePreparationRecoveryFencesOldExecutor(t *testing.T) {
	client, session, taskID := workspaceStoreFixture(t)
	old, err := client.BeginWorkspaceAccess(t.Context(), session.Id, taskID, "result.glb", time.Minute)
	require.NoError(t, err)
	_, err = client.db.Exec(t.Context(), `UPDATE session_workspace_access SET expires_at = clock_timestamp() - interval '1 second' WHERE grant_id = $1`, old.GrantID)
	require.NoError(t, err)
	restarted := NewClient(client.db, "public")
	require.NoError(t, restarted.RecoverWorkspacePreparation(t.Context()))
	current, err := restarted.BeginWorkspaceAccess(t.Context(), session.Id, taskID, "result.glb", time.Minute)
	require.NoError(t, err)
	require.NotEqual(t, old.GrantID, current.GrantID)
	require.ErrorIs(t, client.IssueWorkspaceRead(t.Context(), old), ErrConflict)
	require.NoError(t, client.ReleaseWorkspacePreparation(t.Context(), old))
	require.NoError(t, restarted.IssueWorkspaceRead(t.Context(), current))
}

func TestWorkspaceCleanupRecoveryAndIssuedUncertainty(t *testing.T) {
	for _, phase := range []string{"READING", "CLEANUP", "QUIESCING"} {
		t.Run(phase, func(t *testing.T) {
			client, session, taskID := workspaceStoreFixture(t)
			old, err := client.BeginWorkspaceAccess(t.Context(), session.Id, taskID, "result.glb", time.Minute)
			require.NoError(t, err)
			require.NoError(t, client.IssueWorkspaceRead(t.Context(), old))
			if phase != "READING" {
				require.NoError(t, client.CompleteWorkspaceRead(t.Context(), old))
			}
			if phase == "QUIESCING" {
				require.NoError(t, client.IssueWorkspaceQuiescence(t.Context(), old, 3))
			}
			_, err = client.db.Exec(t.Context(), `UPDATE session_workspace_access SET expires_at = clock_timestamp() - interval '1 second', lease_expires_at = clock_timestamp() - interval '1 second' WHERE grant_id = $1`, old.GrantID)
			require.NoError(t, err)
			restarted := NewClient(client.db, "public")
			require.NoError(t, restarted.RecoverWorkspacePreparation(t.Context()))
			_, err = restarted.ConsumeWorkspaceGrant(t.Context(), session.Id, old.GrantID.String(), old.Path)
			require.ErrorIs(t, err, ErrNotFound)
			current, err := restarted.ClaimWorkspaceCleanup(t.Context())
			if phase == "CLEANUP" {
				require.NoError(t, err)
				require.NotEqual(t, old.ExecutorID, current.ExecutorID)
				require.ErrorIs(t, client.IssueWorkspaceQuiescence(t.Context(), old, 3), ErrConflict)
				require.NoError(t, restarted.IssueWorkspaceQuiescence(t.Context(), current, 3))
			} else {
				require.ErrorIs(t, err, ErrNotFound)
			}
			require.ErrorIs(t, restarted.ReserveSessionDispatch(t.Context(), session.Id, uuid.New(), "next"), ErrDispatchBusy)
		})
	}
}

func TestWorkspaceAccessCannotRaceClaimedQuiescence(t *testing.T) {
	client, session, taskID := workspaceStoreFixture(t)
	_, err := client.ClaimSessionQuiescence(t.Context())
	require.NoError(t, err)
	_, err = client.BeginWorkspaceAccess(t.Context(), session.Id, taskID, "result.glb", time.Minute)
	require.ErrorIs(t, err, ErrFailedPrecondition)
}
