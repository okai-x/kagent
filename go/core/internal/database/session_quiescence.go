package database

import (
	"context"
	"fmt"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

// SessionQuiescence grants one worker a bounded read-only preparation claim.
// IssueSessionQuiescence must authorize the runtime mutation before it is sent.
type SessionQuiescence struct {
	Session    *apiv1.Session
	TaskID     string
	State      a2a.TaskState
	Version    int64
	ExecutorID uuid.UUID
}

// HasPendingSessionQuiescence includes ready sessions temporarily blocked by
// dispatch, checkpoints, lifecycle operations, row locks, or preparation leases.
// Issued work is excluded: its runtime outcome cannot safely be retried.
func (c *Client) HasPendingSessionQuiescence(ctx context.Context) (bool, error) {
	return queryOne(ctx, c.db, `
		SELECT EXISTS (
			SELECT 1 FROM session_task_event e JOIN session_record i ON i.history_id = e.history_id
			WHERE e.published AND e.quiescence_pending = TRUE
			  AND (e.quiescence_executor_id IS NULL OR e.quiescence_phase = 'PREPARING')
			  AND i.state = 'RUNTIME_STATE_READY'
		)
	`, pgx.RowTo[bool])
}

// ClaimSessionQuiescence claims an idle session at its latest settled task
// version. A new turn supersedes unclaimed idle work; a claim blocks task writes,
// checkpoints, and explicit lifecycle operations until it finishes. Missing work
// returns ErrNotFound. Only read-only preparation can be reassigned after its
// lease expires; the old executor must fail Issue before making a runtime call.
func (c *Client) ClaimSessionQuiescence(ctx context.Context) (*SessionQuiescence, error) {
	lifetime := RuntimeOperationTimeout
	if deadline, ok := ctx.Deadline(); ok {
		lifetime = min(lifetime, time.Until(deadline))
	}
	var result *SessionQuiescence
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		type candidate struct {
			SessionID string
			TaskID    string
			Sequence  int64
		}
		row, err := queryOne(ctx, tx, `
			SELECT i.id::text AS session_id, e.task_id, e.sequence
			FROM session_task_event e JOIN session_record i ON i.history_id = e.history_id
			WHERE e.published AND e.quiescence_pending = TRUE
			  AND (e.quiescence_executor_id IS NULL OR e.quiescence_phase = 'PREPARING')
			  AND (e.quiescence_expires_at IS NULL OR e.quiescence_expires_at <= clock_timestamp())
			  AND i.state = 'RUNTIME_STATE_READY'
			  AND i.operation = 'RUNTIME_OPERATION_NONE'
			  AND (i.dispatch_expires_at IS NULL OR i.dispatch_expires_at <= clock_timestamp())
			  AND NOT EXISTS (SELECT 1 FROM session_checkpoint WHERE source_session_id = i.id AND state = 'CREATING')
			ORDER BY e.sequence LIMIT 1 FOR UPDATE OF i SKIP LOCKED
		`, pgx.RowToStructByName[candidate])
		if err != nil {
			return notFoundOr(err)
		}
		session, err := readSession(ctx, tx, row.SessionID)
		if err != nil {
			return err
		}
		if session.State != "RUNTIME_STATE_READY" || session.Operation != "RUNTIME_OPERATION_NONE" {
			return ErrNotFound
		}
		stored, err := readSessionTask(ctx, tx, session.HistoryID, row.TaskID)
		if err != nil {
			return err
		}
		task, err := unmarshalSessionTask(stored.Data)
		if err != nil {
			return err
		}
		value, err := toSession(session)
		if err != nil {
			return err
		}
		result = &SessionQuiescence{Session: value, TaskID: row.TaskID, State: task.Status.State, Version: row.Sequence, ExecutorID: uuid.New()}
		// Recheck after locking: the candidate query's snapshot may predate a
		// new turn that committed just before we acquired the session lock.
		tag, err := tx.Exec(ctx, `
			UPDATE session_task_event SET quiescence_executor_id = $2,
			    quiescence_phase = 'PREPARING',
			    quiescence_expires_at = clock_timestamp() + $4 * interval '1 millisecond'
			WHERE sequence = $1 AND published AND quiescence_pending
			  AND (quiescence_executor_id IS NULL OR quiescence_phase = 'PREPARING')
			  AND (quiescence_expires_at IS NULL OR quiescence_expires_at <= clock_timestamp())
			  AND NOT EXISTS (SELECT 1 FROM session WHERE id = $3 AND dispatch_expires_at > clock_timestamp())
			  AND NOT EXISTS (SELECT 1 FROM session_checkpoint WHERE source_session_id = $3 AND state = 'CREATING')
		`, result.Version, result.ExecutorID, session.ID, lifetime.Milliseconds()+1)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrNotFound
		}
		return nil
	})
	return result, err
}

// IssueSessionQuiescence fences stale preparers and records the immutable actor
// UID and observed snapshot generation before permitting a single suspend call.
// Issued work never expires: Substrate has no request ID or generation fence.
func (c *Client) IssueSessionQuiescence(ctx context.Context, work *SessionQuiescence, actorUID string, generation int32) error {
	if work == nil || work.ExecutorID == uuid.Nil || actorUID == "" || generation < 0 {
		return ErrFailedPrecondition
	}
	tag, err := c.db.Exec(ctx, `
		UPDATE session_task_event SET quiescence_phase = 'ISSUED', quiescence_expires_at = NULL,
		    quiescence_actor_uid = $4, quiescence_snapshot_generation = $5
		WHERE sequence = $1 AND task_id = $2 AND quiescence_executor_id = $3
		    AND quiescence_pending AND quiescence_phase = 'PREPARING'
		    AND quiescence_expires_at > clock_timestamp()
		    AND history_id = (SELECT history_id FROM session WHERE id = $6 AND actor_uid = $4)
	`, work.Version, work.TaskID, work.ExecutorID, actorUID, generation, work.Session.Id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

// ReleaseSessionQuiescence releases only failed read-only preparation and leaves
// a short retry delay. It never clears issued intent or another executor's claim.
func (c *Client) ReleaseSessionQuiescence(ctx context.Context, work *SessionQuiescence) error {
	if work == nil || work.ExecutorID == uuid.Nil {
		return ErrFailedPrecondition
	}
	_, err := c.db.Exec(ctx, `
		UPDATE session_task_event SET quiescence_executor_id = NULL, quiescence_phase = NULL,
		    quiescence_expires_at = clock_timestamp() + $4 * interval '1 millisecond'
		WHERE sequence = $1 AND task_id = $2 AND quiescence_executor_id = $3
		    AND quiescence_pending AND quiescence_phase = 'PREPARING'
		    AND history_id = (SELECT history_id FROM session WHERE id = $5)
	`, work.Version, work.TaskID, work.ExecutorID, time.Second.Milliseconds(), work.Session.Id)
	return err
}

// FinishSessionQuiescence records a claimed suspension outcome and releases
// the session for new execution. Every settled task requires the matching snapshot.
// Retries are harmless, and a stale claim cannot release another operation.
// Task state and history remain readable throughout lifecycle work.
func (c *Client) FinishSessionQuiescence(ctx context.Context, work *SessionQuiescence, snapshot *SessionTaskSnapshot) error {
	if work == nil || work.ExecutorID == uuid.Nil {
		return fmt.Errorf("claimed task boundary is required")
	}
	if snapshot == nil || snapshot.URI == "" || snapshot.Atespace == "" || snapshot.Fidelity == "" {
		return fmt.Errorf("task quiescence requires a runtime snapshot")
	}
	return c.withTx(ctx, func(tx pgx.Tx) error {
		session, err := lockSession(ctx, tx, work.Session.Id)
		if err != nil {
			return notFoundOr(err)
		}
		if session.State == "RUNTIME_STATE_DELETED" {
			return ErrNotFound
		}
		pending, err := queryOne(ctx, tx, `
			SELECT quiescence_pending FROM session_task_event
			WHERE sequence = $1 AND history_id = $2 AND task_id = $3 AND quiescence_executor_id = $4
		`, pgx.RowTo[bool], work.Version, session.HistoryID, work.TaskID, work.ExecutorID)
		if err != nil {
			return notFoundOr(err)
		}
		if !pending {
			return nil
		}
		stored, err := readSessionTask(ctx, tx, session.HistoryID, work.TaskID)
		if err != nil {
			return err
		}
		if stored.State != string(work.State) {
			return ErrConflict
		}
		if snapshot != nil {
			if err := execSQL(ctx, tx, `
				UPDATE session_task SET snapshot_atespace = $3, snapshot_uri = $4,
				    snapshot_fidelity = $5, history_sequence = $6
				WHERE history_id = $1 AND id = $2
			`, session.HistoryID, work.TaskID, snapshot.Atespace, snapshot.URI, snapshot.Fidelity, work.Version); err != nil {
				return err
			}
			if err := execSQL(ctx, tx, `
				UPDATE session_task_event SET snapshot_atespace = $2, snapshot_uri = $3, snapshot_fidelity = $4
				WHERE sequence = $1
			`, work.Version, snapshot.Atespace, snapshot.URI, snapshot.Fidelity); err != nil {
				return err
			}
		}
		return execSQL(ctx, tx, `
			UPDATE session_task_event SET quiescence_pending = FALSE WHERE sequence = $1
		`, work.Version)
	})
}
