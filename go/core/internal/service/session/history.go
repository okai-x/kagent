package session

import (
	"context"
	"errors"

	"github.com/google/uuid"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

// ExportHistory is an owner-only read. It neither provisions a runtime nor
// projects/decodes events, and preserves native bytes unknown to this server.
func (s *Service) ExportHistory(ctx context.Context, request *apiv1.ExportSessionHistoryRequest) (*apiv1.ExportSessionHistoryResponse, error) {
	if err := validateHistoryRequest(request); err != nil {
		return nil, err
	}
	identity, ok := auth.AuthSessionFrom(ctx)
	if !ok {
		return nil, serviceerrors.NewUnauthenticated("History export requires authentication", nil)
	}
	if _, shared := auth.ShareContextFrom(ctx); shared || identity.Principal().RuntimeBinding != nil {
		return nil, serviceerrors.NewPermissionDenied("History export requires the session owner", nil)
	}
	session, err := s.getAuthorized(ctx, request.SessionId, auth.VerbGet)
	if err != nil {
		return nil, err
	}
	size := request.PageSize
	if size == 0 {
		size = maxPageSize
	}
	result, err := s.store.ExportSessionHistory(ctx, session.Creator, &apiv1.ExportSessionHistoryRequest{
		SessionId: session.Id, Cutoff: request.Cutoff, AfterSequence: request.AfterSequence, PageSize: size,
	})
	switch {
	case errors.Is(err, database.ErrNotFound):
		return nil, serviceerrors.NewNotFound("Session not found", err)
	case errors.Is(err, database.ErrFailedPrecondition):
		return nil, serviceerrors.NewFailedPrecondition("History boundary is no longer available", err)
	case errors.Is(err, database.ErrHistoryEventTooLarge):
		return nil, serviceerrors.NewResourceExhausted("History event exceeds the 8 MiB export limit", err)
	case err != nil:
		return nil, serviceerrors.NewInternal("Failed to export Session history", err)
	default:
		return result, nil
	}
}

func validateHistoryRequest(request *apiv1.ExportSessionHistoryRequest) error {
	if request == nil || request.AfterSequence < 0 || request.PageSize < 0 || request.PageSize > maxPageSize {
		return serviceerrors.NewInvalidArgument("Invalid history page bounds", nil)
	}
	if cutoff := request.Cutoff; cutoff != nil {
		if uuid.Validate(cutoff.HistoryId) != nil || cutoff.ThroughSequence < request.AfterSequence {
			return serviceerrors.NewInvalidArgument("Invalid history cutoff", nil)
		}
	} else if request.AfterSequence != 0 {
		return serviceerrors.NewInvalidArgument("A history cutoff is required to resume an export", nil)
	}
	return nil
}
