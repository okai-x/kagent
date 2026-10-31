package session

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

type historyTestStore struct {
	serviceTestStore
	request *apiv1.ExportSessionHistoryRequest
	owner   string
	err     error
}

func (s *historyTestStore) ExportSessionHistory(_ context.Context, owner string, request *apiv1.ExportSessionHistoryRequest) (*apiv1.ExportSessionHistoryResponse, error) {
	s.owner, s.request = owner, request
	return &apiv1.ExportSessionHistoryResponse{Cutoff: request.Cutoff}, s.err
}

func TestHistoryExportCallerAndErrorBoundary(t *testing.T) {
	id := uuid.NewString()
	runtime := auth.AuthSessionTo(t.Context(), &authimpl.SimpleSession{P: auth.Principal{ID: "actor", RuntimeBinding: &auth.RuntimeBinding{SessionID: id, CreatorID: "alice"}}})
	for _, test := range []struct {
		name      string
		ctx       context.Context
		policyErr error
		storeErr  error
		code      serviceerrors.Code
	}{
		{name: "owner", ctx: serviceTestContext("alice")},
		{name: "controller", ctx: auth.AuthSessionTo(t.Context(), auth.ControlPlaneSession{})},
		{name: "share", ctx: auth.ShareContextTo(serviceTestContext("visitor"), &auth.ShareContext{SessionID: id, UserID: "alice"}), code: serviceerrors.CodePermissionDenied},
		{name: "runtime", ctx: runtime, code: serviceerrors.CodePermissionDenied},
		{name: "anonymous", ctx: t.Context(), code: serviceerrors.CodeUnauthenticated},
		{name: "policy denied", ctx: serviceTestContext("alice"), policyErr: errors.New("denied"), code: serviceerrors.CodePermissionDenied},
		{name: "source removed", ctx: serviceTestContext("alice"), storeErr: database.ErrNotFound, code: serviceerrors.CodeNotFound},
		{name: "stale cutoff", ctx: serviceTestContext("alice"), storeErr: database.ErrFailedPrecondition, code: serviceerrors.CodeFailedPrecondition},
		{name: "oversized event", ctx: serviceTestContext("alice"), storeErr: database.ErrHistoryEventTooLarge, code: serviceerrors.CodeResourceExhausted},
		{name: "internal failure", ctx: serviceTestContext("alice"), storeErr: errors.New("private database details"), code: serviceerrors.CodeInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &historyTestStore{serviceTestStore: serviceTestStore{getResult: &apiv1.Session{Id: id, Creator: "alice"}}, err: test.storeErr}
			service := NewService(store, serviceTestAuthorizer{err: test.policyErr}, nil)
			_, err := service.ExportHistory(test.ctx, &apiv1.ExportSessionHistoryRequest{SessionId: id})
			require.Equal(t, test.code, serviceerrors.CodeOf(err))
			if test.code == "" {
				require.Equal(t, "alice", store.owner)
				require.Equal(t, int32(100), store.request.PageSize)
			} else if test.storeErr == nil {
				require.Nil(t, store.request, "denied callers must not read raw bytes")
			}
			if test.code == serviceerrors.CodeInternal {
				require.NotContains(t, serviceerrors.MessageOf(err), "private database details")
			}
		})
	}
}

func TestHistoryExportDirectValidation(t *testing.T) {
	for _, request := range []*apiv1.ExportSessionHistoryRequest{
		nil, {}, {SessionId: uuid.NewString(), AfterSequence: 1},
		{SessionId: uuid.NewString(), AfterSequence: -1},
		{SessionId: uuid.NewString(), PageSize: -1},
		{SessionId: uuid.NewString(), PageSize: 101},
		{SessionId: uuid.NewString(), Cutoff: &apiv1.SessionHistoryCutoff{HistoryId: "bad"}},
		{SessionId: uuid.NewString(), Cutoff: &apiv1.SessionHistoryCutoff{HistoryId: uuid.NewString(), ThroughSequence: -1}},
		{SessionId: uuid.NewString(), AfterSequence: 2, Cutoff: &apiv1.SessionHistoryCutoff{HistoryId: uuid.NewString(), ThroughSequence: 1}},
	} {
		_, err := NewService(&serviceTestStore{}, serviceTestAuthorizer{}, nil).ExportHistory(serviceTestContext("alice"), request)
		require.Equal(t, serviceerrors.CodeInvalidArgument, serviceerrors.CodeOf(err))
	}
}
