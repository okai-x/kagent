package sandbox

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

func TestSandboxExpirationIgnoresStaleCandidateAfterRenewal(t *testing.T) {
	service, ctx, actors := serviceFixture(t)
	created, err := service.Create(ctx, createRequest())
	require.NoError(t, err)
	deadline := created.ExpiresAt.AsTime().Add(time.Hour).Truncate(time.Microsecond)
	_, err = service.Renew(ctx, &apiv1.RenewSandboxRequest{SandboxId: created.Id, ExpiresAt: timestamppb.New(deadline)})
	require.NoError(t, err)
	calls := actors.observedCalls()

	// Dispatch an ID selected by the expiration scan before renewal committed.
	require.NoError(t, service.expire(ctx, created.Id))
	require.Equal(t, calls, actors.observedCalls())
	current, err := service.Get(ctx, created.Id)
	require.NoError(t, err)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_READY, current.State)
	require.Equal(t, deadline, current.ExpiresAt.AsTime())
	deleted, err := service.Delete(ctx, created.Id)
	require.NoError(t, err)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_DELETED, deleted.State)
}
