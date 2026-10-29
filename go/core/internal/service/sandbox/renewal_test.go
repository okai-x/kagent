package sandbox

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

type renewalActor struct{}

func (renewalActor) Principal() auth.Principal {
	return auth.Principal{ID: "alice", RuntimeBinding: &auth.RuntimeBinding{SessionID: "session", CreatorID: "alice"}}
}

func TestSandboxRenewalAuthorizesOwnerAndDoesNotCallRuntime(t *testing.T) {
	service, ctx, actors := serviceFixture(t)
	created, err := service.Create(ctx, createRequest())
	require.NoError(t, err)
	calls := actors.observedCalls()
	request := &apiv1.RenewSandboxRequest{SandboxId: created.Id, ExpiresAt: timestamppb.New(time.Now().Add(2 * time.Hour).Truncate(time.Microsecond))}
	for range 2 {
		result, err := service.Renew(ctx, request)
		require.NoError(t, err)
		require.Equal(t, request.ExpiresAt.AsTime(), result.ExpiresAt.AsTime())
		require.Equal(t, created.State, result.State)
	}
	_, err = service.Renew(auth.AuthSessionTo(t.Context(), testSession("bob")), request)
	require.True(t, serviceerrors.IsCode(err, serviceerrors.CodeNotFound))
	_, err = service.Renew(auth.AuthSessionTo(t.Context(), renewalActor{}), request)
	require.True(t, serviceerrors.IsCode(err, serviceerrors.CodePermissionDenied))
	_, err = service.Renew(ctx, &apiv1.RenewSandboxRequest{SandboxId: created.Id})
	require.True(t, serviceerrors.IsCode(err, serviceerrors.CodeInvalidArgument))
	request.ExpiresAt = timestamppb.New(time.Now().Add(25 * time.Hour))
	_, err = service.Renew(ctx, request)
	require.True(t, serviceerrors.IsCode(err, serviceerrors.CodeFailedPrecondition))
	require.Equal(t, calls, actors.observedCalls())
}
