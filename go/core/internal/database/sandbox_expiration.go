package database

import (
	"context"

	"github.com/jackc/pgx/v5"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

// BeginSandboxExpiration rechecks a GC candidate under the same row lock as
// renewal and persists deletion intent atomically. A renewed candidate returns
// nil without changing lifecycle state. Missing rows and active attempts retain
// the ordinary lifecycle operation errors; callers must not execute a nil claim.
func (c *Client) BeginSandboxExpiration(ctx context.Context, id string) (*SandboxOperation, error) {
	var result *SandboxOperation
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockSandbox(ctx, tx, id)
		if err != nil {
			return notFoundOr(err)
		}
		expired, err := queryOne(ctx, tx, `SELECT expires_at <= clock_timestamp() FROM sandbox WHERE id = $1`, pgx.RowTo[bool], id)
		if err != nil || !expired {
			return err
		}
		result, err = prepareSandboxOperation(ctx, tx, row, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
		return err
	})
	return result, err
}

// prepareSandboxOperation persists lifecycle intent while the caller holds the
// sandbox row lock. An active executor prevents supersession; existing matching
// operations retain their identity so the runtime can safely retry them.
func prepareSandboxOperation(ctx context.Context, tx pgx.Tx, row sandboxRow, kind apiv1.RuntimeOperation) (*SandboxOperation, error) {
	needed, err := sandboxOperationNeeded(row.runtimeInstanceRow, kind)
	if err != nil {
		return nil, err
	}
	if needed {
		if row.ExecutorID != nil {
			active, err := queryOne(ctx, tx, `SELECT executor_expires_at > clock_timestamp() FROM runtime_instance WHERE id = $1`, pgx.RowTo[bool], row.ID)
			if err != nil {
				return nil, err
			}
			if active {
				return nil, ErrConflict
			}
		}
		row, err = beginSandboxOperation(ctx, tx, row, kind)
		if err != nil {
			return nil, err
		}
	}
	instance, err := row.sandbox()
	if err != nil {
		return nil, err
	}
	result := runtimeOperationFor(row.runtimeInstanceRow, instance)
	return &result, nil
}
