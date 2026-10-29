package database

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

func TestSandboxRenewalKeepsDeadlineAndOwner(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	input, options := sandboxFixture(t, client, "renewal")
	created, _, err := client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	ready := finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	deadline := ready.ExpiresAt.AsTime().Add(time.Hour).Truncate(time.Microsecond)

	_, err = client.RenewSandbox(ctx, ready.Id, "other-owner", deadline, 24*time.Hour)
	require.ErrorIs(t, err, ErrNotFound)
	for _, requested := range []time.Time{deadline, deadline, deadline.Add(-time.Minute)} {
		renewed, err := client.RenewSandbox(ctx, ready.Id, ready.Creator, requested, 24*time.Hour)
		require.NoError(t, err)
		require.Equal(t, deadline, renewed.ExpiresAt.AsTime())
		require.Equal(t, ready.State, renewed.State)
		require.Equal(t, ready.Operation, renewed.Operation)
	}
	current, err := client.GetSandbox(ctx, ready.Id, ready.Creator)
	require.NoError(t, err)
	require.Equal(t, deadline, current.ExpiresAt.AsTime())
	for _, invalid := range []time.Time{time.Now().Add(-time.Minute), time.Now().Add(25 * time.Hour)} {
		_, err := client.RenewSandbox(ctx, ready.Id, ready.Creator, invalid, 24*time.Hour)
		require.ErrorIs(t, err, ErrFailedPrecondition)
	}
}

func TestSandboxRenewalRejectsExpiredAndDeleting(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	for _, state := range []string{"expired", "deleting", "deleted"} {
		t.Run(state, func(t *testing.T) {
			input, options := sandboxFixture(t, client, "reject-renewal")
			created, _, err := client.CreateSandbox(t.Context(), input, uuid.NewString(), options)
			require.NoError(t, err)
			if state == "expired" {
				// Simulate elapsed wall time without sleeping or reviving an expired lease.
				_, err = client.db.Exec(t.Context(), "UPDATE sandbox SET expires_at = clock_timestamp() - interval '1 second' WHERE id = $1", created.Id)
				require.NoError(t, err)
			} else {
				_, err = client.BeginSandboxOperation(t.Context(), created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
				require.NoError(t, err)
				if state == "deleted" {
					finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
				}
			}
			_, err = client.RenewSandbox(t.Context(), created.Id, created.Creator, time.Now().Add(time.Hour), 24*time.Hour)
			require.ErrorIs(t, err, ErrFailedPrecondition)
		})
	}
}

func TestSandboxConcurrentRenewalCannotShortenLease(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	input, options := sandboxFixture(t, client, "concurrent-renewal")
	created, _, err := client.CreateSandbox(t.Context(), input, "create", options)
	require.NoError(t, err)
	longest := created.ExpiresAt.AsTime().Add(2 * time.Hour).Truncate(time.Microsecond)
	errors := make(chan error, 2)
	var workers sync.WaitGroup
	for _, deadline := range []time.Time{longest, longest.Add(-time.Hour)} {
		workers.Go(func() {
			_, err := client.RenewSandbox(t.Context(), created.Id, created.Creator, deadline, 24*time.Hour)
			errors <- err
		})
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	current, err := client.GetSandbox(t.Context(), created.Id, created.Creator)
	require.NoError(t, err)
	require.Equal(t, longest, current.ExpiresAt.AsTime())
}
