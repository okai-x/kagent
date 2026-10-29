package session

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

type workspaceReadFunc func(context.Context, *database.WorkspaceAccess, io.Writer) (bool, error)

func (f workspaceReadFunc) Read(ctx context.Context, work *database.WorkspaceAccess, output io.Writer) (bool, error) {
	return f(ctx, work, output)
}

func workspaceFixture(t *testing.T) (*lifecycleTestStore, *ActorWorkflow, *apiv1.ReadWorkspaceFileRequest, context.Context) {
	t.Helper()
	store, session := lifecycleFixture(t)
	actors := &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}}
	workflow := NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Hour)
	session, err := workflow.Create(t.Context(), session)
	require.NoError(t, err)
	boundary := stageQuiescenceTask(t, store.Client, session, a2a.TaskStateCompleted)
	require.NoError(t, store.SettleSessionTask(t.Context(), session.Id, boundary.TaskId, boundary.Version))
	request := &apiv1.ReadWorkspaceFileRequest{SessionId: session.Id, ExpectedHeadTaskId: boundary.TaskId, Path: "result.glb"}
	return store, workflow, request, auth.AuthSessionTo(t.Context(), serviceTestSession{userID: "alice"})
}

func TestWorkspaceReadReleasesOnlyAfterSnapshot(t *testing.T) {
	store, workflow, request, ctx := workspaceFixture(t)
	var service *WorkspaceService
	var grant string
	reader := workspaceReadFunc(func(_ context.Context, work *database.WorkspaceAccess, output io.Writer) (bool, error) {
		grant = work.GrantID.String()
		runtimeCtx := settlementContext(t, work.Session)
		_, err := service.AuthorizeRead(runtimeCtx, grant, work.Path)
		require.NoError(t, err)
		_, err = service.AuthorizeRead(runtimeCtx, grant, work.Path)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.ErrorIs(t, store.ReserveSessionDispatch(t.Context(), work.Session.Id, uuid.New(), "next"), database.ErrDispatchBusy)
		_, err = output.Write([]byte("binary-glb"))
		return true, err
	})
	service = NewWorkspaceService(store, NewService(store, serviceTestAuthorizer{}, workflow), workflow, reader, time.Second)
	var output bytes.Buffer
	err := service.Read(ctx, request, func(chunk []byte) error {
		_, snapshot, err := store.ReserveSessionCheckpoint(t.Context(), &apiv1.Checkpoint{
			Id: uuid.NewString(), SessionId: request.SessionId, HeadTaskId: request.ExpectedHeadTaskId,
		}, "alice", "workspace-checkpoint")
		require.NoError(t, err, "public bytes must wait until snapshot is durable and admission is released")
		require.NotEmpty(t, snapshot.URI)
		_, err = output.Write(chunk)
		return err
	})
	require.NoError(t, err)
	require.Equal(t, "binary-glb", output.String())
	_, err = service.AuthorizeRead(ctx, grant, request.Path)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "owner credentials cannot consume runtime grants")
}

func TestWorkspaceReadKnownErrorsStillCleanup(t *testing.T) {
	for _, code := range []codes.Code{codes.NotFound, codes.Unimplemented, codes.FailedPrecondition, codes.ResourceExhausted} {
		t.Run(code.String(), func(t *testing.T) {
			store, workflow, request, ctx := workspaceFixture(t)
			reader := workspaceReadFunc(func(context.Context, *database.WorkspaceAccess, io.Writer) (bool, error) {
				return true, status.Error(code, "private failure")
			})
			service := NewWorkspaceService(store, NewService(store, serviceTestAuthorizer{}, workflow), workflow, reader, time.Second)
			err := service.Read(ctx, request, func([]byte) error { t.Fatal("failed download exposed bytes"); return nil })
			require.Error(t, err)
			if code == codes.NotFound {
				detail := status.Convert(err).Details()[0].(*errdetails.ErrorInfo)
				require.Equal(t, "WORKSPACE_FILE_NOT_FOUND", detail.Reason)
				require.Equal(t, "kagent.dev", detail.Domain)
			}
			if code == codes.Unimplemented {
				require.Equal(t, serviceerrors.CodeFailedPrecondition, serviceerrors.CodeOf(err))
			}
			require.NoError(t, store.ReserveSessionDispatch(t.Context(), request.SessionId, uuid.New(), "next"))
		})
	}
}

func TestWorkspaceReadClientCancelCompletesCleanup(t *testing.T) {
	store, workflow, request, ctx := workspaceFixture(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader := workspaceReadFunc(func(runtimeCtx context.Context, _ *database.WorkspaceAccess, output io.Writer) (bool, error) {
		cancel()
		require.NoError(t, runtimeCtx.Err(), "public disconnect must not abandon private read cleanup")
		_, err := output.Write([]byte("complete"))
		return true, err
	})
	service := NewWorkspaceService(store, NewService(store, serviceTestAuthorizer{}, workflow), workflow, reader, time.Second)
	err := service.Read(ctx, request, func([]byte) error { t.Fatal("canceled download exposed bytes"); return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, store.ReserveSessionDispatch(t.Context(), request.SessionId, uuid.New(), "next"))
}

func TestWorkspaceUnknownEffectsStayClosed(t *testing.T) {
	for _, failure := range []string{"read", "suspend"} {
		t.Run(failure, func(t *testing.T) {
			store, workflow, request, ctx := workspaceFixture(t)
			if failure == "suspend" {
				workflow.actors = &quiescenceFailureActors{retryTestActors: &retryTestActors{lifecycleTestActors: workflow.actors.(*lifecycleTestActors)}, beforeEffect: true}
			}
			reader := workspaceReadFunc(func(context.Context, *database.WorkspaceAccess, io.Writer) (bool, error) {
				if failure == "read" {
					return false, status.Error(codes.Unavailable, "lost read response")
				}
				return true, nil
			})
			service := NewWorkspaceService(store, NewService(store, serviceTestAuthorizer{}, workflow), workflow, reader, time.Second)
			err := service.Read(ctx, request, func([]byte) error { t.Fatal("unresolved download exposed bytes"); return nil })
			require.Equal(t, serviceerrors.CodeUnavailable, serviceerrors.CodeOf(err))
			require.NoError(t, service.recover(t.Context()), "restart does not repeat uncertain effects")
			require.ErrorIs(t, store.ReserveSessionDispatch(t.Context(), request.SessionId, uuid.New(), "next"), database.ErrDispatchBusy)
		})
	}
}

type retryWorkspaceFinish struct {
	*lifecycleTestStore
	attempts int
}

func (s *retryWorkspaceFinish) FinishWorkspaceAccess(ctx context.Context, work *database.WorkspaceAccess, snapshot *database.SessionTaskSnapshot) error {
	s.attempts++
	if s.attempts == 1 {
		return errors.New("temporary database loss")
	}
	return s.Client.FinishWorkspaceAccess(ctx, work, snapshot)
}

func TestWorkspaceKnownSnapshotPersistenceRetries(t *testing.T) {
	store, workflow, request, ctx := workspaceFixture(t)
	writes := &retryWorkspaceFinish{lifecycleTestStore: store}
	reader := workspaceReadFunc(func(context.Context, *database.WorkspaceAccess, io.Writer) (bool, error) { return true, nil })
	service := NewWorkspaceService(writes, NewService(store, serviceTestAuthorizer{}, workflow), workflow, reader, time.Second)
	require.NoError(t, service.Read(ctx, request, func([]byte) error { return nil }))
	require.Equal(t, 2, writes.attempts)
	require.NoError(t, store.ReserveSessionDispatch(t.Context(), request.SessionId, uuid.New(), "next"))
}

func TestWorkspaceAuthorizationBeforeAdmission(t *testing.T) {
	store, workflow, request, ctx := workspaceFixture(t)
	service := NewWorkspaceService(store, NewService(store, serviceTestAuthorizer{}, workflow), workflow, nil, time.Second)
	for _, denied := range []context.Context{
		t.Context(),
		auth.ShareContextTo(ctx, &auth.ShareContext{SessionID: request.SessionId, UserID: "alice"}),
		settlementContext(t, &apiv1.Session{Id: request.SessionId}),
		auth.AuthSessionTo(t.Context(), serviceTestSession{userID: "other-owner"}),
	} {
		err := service.Read(denied, request, func([]byte) error { t.Fatal("denied download exposed bytes"); return nil })
		require.Error(t, err)
	}
	work, err := store.BeginWorkspaceAccess(t.Context(), request.SessionId, request.ExpectedHeadTaskId, request.Path, time.Minute)
	require.NoError(t, err, "denied callers cannot reserve runtime access")
	require.NoError(t, store.ReleaseWorkspacePreparation(t.Context(), work))
}
