package database

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

// RenewSandbox extends an owner's live lease atomically with lifecycle checks.
// Repeating an absolute deadline never shortens or repeatedly extends the lease.
// Expired and deleting sandboxes cannot be revived. maxTTL bounds each renewal.
func (c *Client) RenewSandbox(ctx context.Context, id, owner string, deadline time.Time, maxTTL time.Duration) (*apiv1.Sandbox, error) {
	if maxTTL <= 0 || maxTTL > 24*time.Hour {
		return nil, ErrFailedPrecondition
	}
	var result *apiv1.Sandbox
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockSandbox(ctx, tx, id)
		if err != nil {
			return notFoundOr(err)
		}
		if row.UserID != owner {
			return ErrNotFound
		}
		now, err := queryOne(ctx, tx, `SELECT clock_timestamp()`, pgx.RowTo[time.Time])
		if err != nil {
			return err
		}
		if !row.ExpiresAt.After(now) || row.State == apiv1.RuntimeState_RUNTIME_STATE_DELETED.String() ||
			row.Operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE.String() ||
			!deadline.After(now) || deadline.After(now.Add(maxTTL)) {
			return ErrFailedPrecondition
		}
		if deadline.After(row.ExpiresAt) {
			if err := execSQL(ctx, tx, `UPDATE sandbox SET expires_at = $2 WHERE id = $1`, id, deadline); err != nil {
				return err
			}
			row.ExpiresAt = deadline
		}
		result, err = row.sandbox()
		if err != nil {
			return err
		}
		result.UpdatedAt = timestamppb.New(now)
		_, err = saveSandboxPayload(ctx, tx, result)
		return err
	})
	return result, err
}
