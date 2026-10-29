package database

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// HasSessionSandboxBinding includes deleted Sandbox tombstones: a workspace
// Session must never regain access to its owner's shared memory partition.
func (c *Client) HasSessionSandboxBinding(ctx context.Context, sessionID string) (bool, error) {
	return queryOne(ctx, c.db, `SELECT EXISTS (SELECT 1 FROM sandbox WHERE session_id = $1)`, pgx.RowTo[bool], sessionID)
}
