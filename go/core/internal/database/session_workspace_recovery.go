package database

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// RecoverWorkspacePreparation revokes abandoned claims that never issued a
// runtime request. It cannot clear READING or QUIESCING uncertainty.
func (c *Client) RecoverWorkspacePreparation(ctx context.Context) error {
	return execSQL(ctx, c.db, `
		UPDATE session_workspace_access SET phase = 'RELEASED'
		WHERE phase = 'PREPARING' AND expires_at <= clock_timestamp()
	`)
}

// ClaimWorkspaceCleanup takes over expired read-only cleanup preparation.
// The original read has definitely completed and its grant is revoked. The
// matching executor must durably issue suspension before any lifecycle call.
func (c *Client) ClaimWorkspaceCleanup(ctx context.Context) (*WorkspaceAccess, error) {
	var result *WorkspaceAccess
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		type candidate struct {
			SessionID      string
			GrantID        uuid.UUID
			TaskID         string
			Version        int64
			ActorUID, Path string
			ExpiresAt      time.Time
		}
		row, err := queryOne(ctx, tx, `
			SELECT a.session_id::text, a.grant_id, a.head_task_id AS task_id,
			    a.head_version AS version, a.actor_uid, a.path, a.expires_at
			FROM session_workspace_access a JOIN session s ON s.id = a.session_id
			WHERE a.phase = 'CLEANUP' AND a.lease_expires_at <= clock_timestamp()
			ORDER BY a.lease_expires_at LIMIT 1 FOR UPDATE OF s, a SKIP LOCKED
		`, pgx.RowToStructByName[candidate])
		if err != nil {
			return notFoundOr(err)
		}
		session, err := readSession(ctx, tx, row.SessionID)
		if err != nil {
			return err
		}
		value, err := toSession(session)
		if err != nil {
			return err
		}
		result = &WorkspaceAccess{Session: value, GrantID: row.GrantID, ExecutorID: uuid.New(), TaskID: row.TaskID,
			Version: row.Version, ActorUID: row.ActorUID, Path: row.Path, ExpiresAt: row.ExpiresAt}
		return execSQL(ctx, tx, `
			UPDATE session_workspace_access SET executor_id = $2,
			    lease_expires_at = clock_timestamp() + interval '2 minutes'
			WHERE grant_id = $1
		`, row.GrantID, result.ExecutorID)
	})
	return result, err
}
