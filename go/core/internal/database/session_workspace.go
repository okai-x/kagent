package database

import (
	"context"
	"errors"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

// WorkspaceAccess is a fenced file download at one settled task boundary.
// The grant is secret and must not be logged or exposed in public responses.
type WorkspaceAccess struct {
	Session    *apiv1.Session
	GrantID    uuid.UUID
	ExecutorID uuid.UUID
	TaskID     string
	Version    int64
	ActorUID   string
	Path       string
	ExpiresAt  time.Time
}

// BeginWorkspaceAccess reserves the current terminal head under the session
// lock. The caller authorizes ownership; no runtime work has occurred yet.
// Existing access, dispatch, native cleanup, lifecycle or checkpoint work blocks
// admission. PREPARING claims may be removed after expiry without waking actors.
func (c *Client) BeginWorkspaceAccess(ctx context.Context, sessionID, taskID, path string, lifetime time.Duration) (*WorkspaceAccess, error) {
	var result *WorkspaceAccess
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockSession(ctx, tx, sessionID)
		if err != nil {
			return notFoundOr(err)
		}
		if row.State != "RUNTIME_STATE_READY" || row.Operation != "RUNTIME_OPERATION_NONE" {
			return ErrFailedPrecondition
		}
		if err := requireSettledRuntime(ctx, tx, row.HistoryID, ""); err != nil {
			return err
		}
		busy, err := queryOne(ctx, tx, `
			SELECT EXISTS (SELECT 1 FROM session_checkpoint WHERE source_session_id = $1 AND state = 'CREATING')
			    OR EXISTS (SELECT 1 FROM session_task WHERE history_id = $2
			        AND state NOT IN ('TASK_STATE_COMPLETED', 'TASK_STATE_FAILED', 'TASK_STATE_CANCELED', 'TASK_STATE_REJECTED'))
		`, pgx.RowTo[bool], row.ID, row.HistoryID)
		if err != nil {
			return err
		}
		if busy || lifetime <= 0 || lifetime > RuntimeOperationTimeout {
			return ErrFailedPrecondition
		}
		type head struct {
			TaskID, State, ActorUID string
			Version                 int64
		}
		current, err := queryOne(ctx, tx, `
			SELECT e.task_id, t.state, s.actor_uid, e.sequence AS version
			FROM session s JOIN session_task_event e ON e.history_id = s.history_id
			JOIN session_task t ON t.history_id = e.history_id AND t.id = e.task_id
			WHERE s.id = $1 ORDER BY e.sequence DESC LIMIT 1
		`, pgx.RowToStructByName[head], row.ID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrFailedPrecondition
		}
		if err != nil {
			return err
		}
		if current.TaskID != taskID || !a2a.TaskState(current.State).Terminal() || current.ActorUID == "" {
			return ErrFailedPrecondition
		}
		session, err := toSession(row)
		if err != nil {
			return err
		}
		result = &WorkspaceAccess{Session: session, GrantID: uuid.New(), ExecutorID: uuid.New(), TaskID: taskID,
			Version: current.Version, ActorUID: current.ActorUID, Path: path}
		result.ExpiresAt, err = queryOne(ctx, tx, `
			INSERT INTO session_workspace_access (session_id, grant_id, executor_id, head_task_id,
			    head_version, actor_uid, path, phase, expires_at, lease_expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, 'PREPARING',
			    clock_timestamp() + $8 * interval '1 millisecond', clock_timestamp() + $8 * interval '1 millisecond')
			ON CONFLICT (session_id) DO UPDATE SET grant_id = EXCLUDED.grant_id, executor_id = EXCLUDED.executor_id,
			    head_task_id = EXCLUDED.head_task_id, head_version = EXCLUDED.head_version, actor_uid = EXCLUDED.actor_uid,
			    path = EXCLUDED.path, phase = 'PREPARING', consumed = FALSE, expires_at = EXCLUDED.expires_at,
			    lease_expires_at = EXCLUDED.lease_expires_at, snapshot_generation = NULL
			WHERE session_workspace_access.phase = 'RELEASED'
			RETURNING expires_at
		`, pgx.RowTo[time.Time], row.ID, result.GrantID, result.ExecutorID, taskID, current.Version, current.ActorUID, path, lifetime.Milliseconds())
		return err
	})
	return result, err
}

// IssueWorkspaceRead fences expired preparation before any call that can wake
// the Actor. READING never expires into an automatic retry or release.
func (c *Client) IssueWorkspaceRead(ctx context.Context, work *WorkspaceAccess) error {
	tag, err := c.db.Exec(ctx, `
		UPDATE session_workspace_access a SET phase = 'READING'
		WHERE grant_id = $1 AND executor_id = $2 AND phase = 'PREPARING'
		    AND expires_at > clock_timestamp()
		    AND actor_uid = (SELECT actor_uid FROM session WHERE id = a.session_id)
	`, work.GrantID, work.ExecutorID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

// ConsumeWorkspaceGrant authorizes one private stream for the verified current
// runtime. Calls with a different path/session, expired or reused grant fail
// identically. Runtime authentication separately verifies the immutable Actor UID.
func (c *Client) ConsumeWorkspaceGrant(ctx context.Context, sessionID, grantID, path string) (time.Time, error) {
	expires, err := queryOne(ctx, c.db, `
		UPDATE session_workspace_access a SET consumed = TRUE
		WHERE session_id = $1 AND grant_id = $2 AND path = $3 AND phase = 'READING'
		    AND NOT consumed AND expires_at > clock_timestamp()
		    AND actor_uid = (SELECT actor_uid FROM session WHERE id = a.session_id)
		RETURNING expires_at
	`, pgx.RowTo[time.Time], sessionID, grantID, path)
	return expires, notFoundOr(err)
}

// ReleaseWorkspacePreparation clears only this executor's unissued claim.
// Issued reads and lifecycle effects remain fenced regardless of deadlines.
func (c *Client) ReleaseWorkspacePreparation(ctx context.Context, work *WorkspaceAccess) error {
	return execSQL(ctx, c.db, `
		UPDATE session_workspace_access SET phase = 'RELEASED' WHERE grant_id = $1 AND executor_id = $2 AND phase = 'PREPARING'
	`, work.GrantID, work.ExecutorID)
}

// CompleteWorkspaceRead revokes the private grant after a definite RPC outcome
// and records resumable cleanup preparation. No further file access is admitted.
func (c *Client) CompleteWorkspaceRead(ctx context.Context, work *WorkspaceAccess) error {
	tag, err := c.db.Exec(ctx, `
		UPDATE session_workspace_access SET phase = 'CLEANUP',
		    lease_expires_at = clock_timestamp() + interval '2 minutes'
		WHERE grant_id = $1 AND executor_id = $2 AND phase = 'READING'
	`, work.GrantID, work.ExecutorID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

// IssueWorkspaceQuiescence durably authorizes exactly one suspend. A lost
// response leaves QUIESCING fenced; observing an Actor alone cannot prove it safe.
func (c *Client) IssueWorkspaceQuiescence(ctx context.Context, work *WorkspaceAccess, generation int32) error {
	tag, err := c.db.Exec(ctx, `
		UPDATE session_workspace_access a SET phase = 'QUIESCING', snapshot_generation = $3
		WHERE grant_id = $1 AND executor_id = $2 AND phase = 'CLEANUP'
		    AND lease_expires_at > clock_timestamp()
		    AND actor_uid = (SELECT actor_uid FROM session WHERE id = a.session_id)
	`, work.GrantID, work.ExecutorID, generation)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

// FinishWorkspaceAccess atomically retains the known snapshot and releases the
// matching download. An unknown or replaced claim cannot change task history.
func (c *Client) FinishWorkspaceAccess(ctx context.Context, work *WorkspaceAccess, snapshot *SessionTaskSnapshot) error {
	if snapshot == nil || snapshot.URI == "" || snapshot.Atespace == "" || snapshot.Fidelity != "SNAPSHOT_FIDELITY_VOLUMES" {
		return ErrFailedPrecondition
	}
	return c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockSession(ctx, tx, work.Session.Id)
		if err != nil {
			return notFoundOr(err)
		}
		tag, err := tx.Exec(ctx, `
			UPDATE session_workspace_access SET phase = 'RELEASED' WHERE session_id = $1 AND grant_id = $2
			    AND executor_id = $3 AND phase IN ('QUIESCING', 'RELEASED')
		`, row.ID, work.GrantID, work.ExecutorID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrConflict
		}
		if err := execSQL(ctx, tx, `
			UPDATE session_task SET snapshot_atespace = $3, snapshot_uri = $4,
			    snapshot_fidelity = $5, history_sequence = $6 WHERE history_id = $1 AND id = $2
		`, row.HistoryID, work.TaskID, snapshot.Atespace, snapshot.URI, snapshot.Fidelity, work.Version); err != nil {
			return err
		}
		return execSQL(ctx, tx, `
			UPDATE session_task_event SET snapshot_atespace = $3, snapshot_uri = $4,
			    snapshot_fidelity = $5, quiescence_pending = FALSE
			WHERE history_id = $1 AND sequence = $2
		`, row.HistoryID, work.Version, snapshot.Atespace, snapshot.URI, snapshot.Fidelity)
	})
}
