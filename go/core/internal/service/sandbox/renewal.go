package sandbox

import (
	"context"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

// Renew extends a lease for the authenticated product owner, never for an Actor.
// It changes only persisted expiration; no runtime mutation or retry is needed.
func (s *Service) Renew(ctx context.Context, request *apiv1.RenewSandboxRequest) (*apiv1.Sandbox, error) {
	principal, err := s.principal(ctx, auth.VerbUpdate, auth.Resource{Type: "Sandbox", Name: request.GetSandboxId()})
	if err != nil {
		return nil, err
	}
	if principal.RuntimeBinding != nil {
		return nil, serviceerrors.NewPermissionDenied("Runtime actors cannot renew sandbox leases", nil)
	}
	if request.GetExpiresAt() == nil || request.GetExpiresAt().CheckValid() != nil {
		return nil, serviceerrors.NewInvalidArgument("A valid sandbox expiration is required", nil)
	}
	instance, err := s.config.Store.RenewSandbox(ctx, request.SandboxId, principal.ID, request.ExpiresAt.AsTime(), s.config.MaxTTL)
	return instance, sandboxError(err)
}
