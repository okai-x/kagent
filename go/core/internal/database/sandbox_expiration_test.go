package database

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

func TestSandboxExpirationDoesNotDeleteRenewedCandidate(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	input, options := sandboxFixture(t, client, "renew-before-expiration")
	created, _, err := client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	ready := finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	deadline := ready.ExpiresAt.AsTime().Add(time.Hour).Truncate(time.Microsecond)
	_, err = client.RenewSandbox(ctx, ready.Id, ready.Creator, deadline, 24*time.Hour)
	require.NoError(t, err)

	// A GC candidate can come from a snapshot preceding the renewal commit.
	op, err := client.BeginSandboxExpiration(ctx, ready.Id)
	require.NoError(t, err)
	require.Nil(t, op)
	current, err := client.GetSandbox(ctx, ready.Id, ready.Creator)
	require.NoError(t, err)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_READY, current.State)
	require.Equal(t, apiv1.RuntimeOperation_RUNTIME_OPERATION_NONE, current.Operation)
	require.Equal(t, deadline, current.ExpiresAt.AsTime())

	// Explicit owner deletion is still unconditional even before expiration.
	op, err = client.BeginSandboxOperation(ctx, ready.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.NoError(t, err)
	require.Equal(t, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE, op.Instance.Operation)
}

func TestSandboxExpirationClaimPreventsRenewalAndRetainsRetryIdentity(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	input, options := sandboxFixture(t, client, "expiration-before-renew")
	created, _, err := client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	// Simulate elapsed wall time without sleeping in the integration test.
	_, err = client.db.Exec(ctx, "UPDATE sandbox SET expires_at = clock_timestamp() - interval '1 second' WHERE id = $1", created.Id)
	require.NoError(t, err)
	candidates, err := client.ListExpiredSandboxes(ctx, "", 100)
	require.NoError(t, err)
	require.Contains(t, candidates, created.Id)
	op, err := client.BeginSandboxExpiration(ctx, created.Id)
	require.NoError(t, err)
	require.NotNil(t, op)
	require.Equal(t, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE, op.Instance.Operation)
	_, err = client.RenewSandbox(ctx, created.Id, created.Creator, time.Now().Add(time.Hour), 24*time.Hour)
	require.ErrorIs(t, err, ErrFailedPrecondition)
	retry, err := client.BeginSandboxExpiration(ctx, created.Id)
	require.NoError(t, err)
	require.Equal(t, op.ID, retry.ID)
	deleted := finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_DELETED, deleted.State)
}
