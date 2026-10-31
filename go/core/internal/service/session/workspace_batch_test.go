package session

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
)

type batchActors struct {
	*lifecycleTestActors
	suspends int
}

func (a *batchActors) SuspendActor(ctx context.Context, space, name string) (*ateapipb.Actor, error) {
	a.suspends++
	return a.lifecycleTestActors.SuspendActor(ctx, space, name)
}
func batchRequest(request *apiv1.ReadWorkspaceFileRequest) *apiv1.ReadWorkspaceFilesRequest {
	return &apiv1.ReadWorkspaceFilesRequest{SessionId: request.SessionId, ExpectedHeadTaskId: request.ExpectedHeadTaskId,
		Paths: []string{"result.glb", "result.blend", "result.png", "solution.py"}}
}

func TestWorkspaceBatchReadsOneBoundaryAndSuspendsOnce(t *testing.T) {
	store, workflow, request, ctx := workspaceFixture(t)
	actors := &batchActors{lifecycleTestActors: workflow.actors.(*lifecycleTestActors)}
	workflow.actors = actors
	var service *WorkspaceService
	grants := map[uuid.UUID]bool{}
	var paths []string
	reader := workspaceReadFunc(func(_ context.Context, work *database.WorkspaceAccess, output io.Writer) (bool, error) {
		require.Equal(t, request.ExpectedHeadTaskId, work.TaskID)
		require.False(t, grants[work.GrantID])
		grants[work.GrantID] = true
		paths = append(paths, work.Path)
		runtimeCtx := settlementContext(t, work.Session)
		_, err := service.AuthorizeRead(runtimeCtx, work.GrantID.String(), work.Path)
		require.NoError(t, err)
		_, err = service.AuthorizeRead(runtimeCtx, work.GrantID.String(), work.Path)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.Zero(t, actors.suspends, "all reads must precede the only suspension")
		require.ErrorIs(t, store.ReserveSessionDispatch(t.Context(), work.Session.Id, uuid.New(), "next"), database.ErrDispatchBusy)
		switch work.Path {
		case "result.blend":
			return true, status.Error(codes.NotFound, "missing")
		case "result.png":
			return true, nil
		default:
			_, err := output.Write([]byte(work.Path))
			return true, err
		}
	})
	service = NewWorkspaceService(store, NewService(store, serviceTestAuthorizer{}, workflow), workflow, reader, time.Second)
	var results []*apiv1.ReadWorkspaceFilesResponse
	err := service.ReadFiles(ctx, batchRequest(request), func(response *apiv1.ReadWorkspaceFilesResponse) error {
		require.Equal(t, 1, actors.suspends)
		_, snapshot, err := store.ReserveSessionCheckpoint(t.Context(), &apiv1.Checkpoint{
			Id: uuid.NewString(), SessionId: request.SessionId, HeadTaskId: request.ExpectedHeadTaskId,
		}, "alice", "batch-checkpoint")
		require.NoError(t, err)
		require.NotEmpty(t, snapshot.URI, "even missing markers require durable cleanup")
		results = append(results, proto.CloneOf(response))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, batchRequest(request).Paths, paths)
	expected := []*apiv1.ReadWorkspaceFilesResponse{
		{Path: "result.glb", Chunk: []byte("result.glb")}, {Path: "result.blend", Missing: true},
		{Path: "result.png"}, {Path: "solution.py", Chunk: []byte("solution.py")},
	}
	require.Len(t, results, len(expected))
	for i, result := range results {
		require.True(t, proto.Equal(expected[i], result), "unexpected response %d: %v", i, result)
	}
	require.Equal(t, 1, actors.suspends)
}

type lostBatchAdvance struct{ *lifecycleTestStore }

func (s *lostBatchAdvance) NextWorkspaceRead(ctx context.Context, work *database.WorkspaceAccess, path string) (*database.WorkspaceAccess, error) {
	_, err := s.Client.NextWorkspaceRead(ctx, work, path)
	if err != nil {
		return nil, err
	}
	return nil, errors.New("database response lost after commit")
}

func TestWorkspaceBatchFailuresNeverExposeEarlierFiles(t *testing.T) {
	for _, kind := range []string{"unknown-read", "known-read", "partial-missing", "cancel", "lost-advance", "unknown-suspend"} {
		t.Run(kind, func(t *testing.T) {
			store, workflow, request, ctx := workspaceFixture(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			actors := &batchActors{lifecycleTestActors: workflow.actors.(*lifecycleTestActors)}
			workflow.actors = actors
			if kind == "unknown-suspend" {
				workflow.actors = &quiescenceFailureActors{retryTestActors: &retryTestActors{lifecycleTestActors: actors.lifecycleTestActors}, beforeEffect: true}
			}
			reads := 0
			reader := workspaceReadFunc(func(privateCtx context.Context, _ *database.WorkspaceAccess, output io.Writer) (bool, error) {
				reads++
				_, err := output.Write([]byte("partial"))
				require.NoError(t, err)
				if kind == "cancel" {
					cancel()
					require.NoError(t, privateCtx.Err())
					return true, nil
				}
				if reads == 2 {
					switch kind {
					case "unknown-read":
						return false, status.Error(codes.Unavailable, "lost read")
					case "known-read":
						return true, status.Error(codes.PermissionDenied, "denied")
					case "partial-missing":
						return true, status.Error(codes.NotFound, "missing after data")
					}
				}
				return true, nil
			})
			var persistence workspaceStore = store
			if kind == "lost-advance" {
				persistence = &lostBatchAdvance{store}
			}
			service := NewWorkspaceService(persistence, NewService(store, serviceTestAuthorizer{}, workflow), workflow, reader, time.Second)
			err := service.ReadFiles(ctx, batchRequest(request), func(*apiv1.ReadWorkspaceFilesResponse) error {
				t.Fatal("failed batch exposed a partial result")
				return nil
			})
			require.Error(t, err)
			require.NoError(t, service.recover(t.Context()))
			if kind == "known-read" || kind == "partial-missing" || kind == "cancel" {
				require.Equal(t, 1, actors.suspends)
				require.NoError(t, store.ReserveSessionDispatch(t.Context(), request.SessionId, uuid.New(), "next"))
			} else {
				require.Zero(t, actors.suspends)
				require.ErrorIs(t, store.ReserveSessionDispatch(t.Context(), request.SessionId, uuid.New(), "next"), database.ErrDispatchBusy)
			}
			expectedReads := 2
			if kind == "cancel" || kind == "lost-advance" {
				expectedReads = 1
			}
			if kind == "unknown-suspend" {
				expectedReads = 4
			}
			require.Equal(t, expectedReads, reads)
		})
	}
}

func TestWorkspaceAdmissionReasonIsOnlyForRejectedBegin(t *testing.T) {
	store, workflow, request, ctx := workspaceFixture(t)
	service := NewWorkspaceService(store, NewService(store, serviceTestAuthorizer{}, workflow), workflow, nil, time.Second)
	request.ExpectedHeadTaskId = "stale-head"
	err := service.ReadFiles(ctx, batchRequest(request), func(*apiv1.ReadWorkspaceFilesResponse) error { t.Fatal("rejected read"); return nil })
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.True(t, proto.Equal(&errdetails.ErrorInfo{Reason: "WORKSPACE_ADMISSION_REJECTED", Domain: "kagent.dev"}, status.Convert(err).Details()[0].(*errdetails.ErrorInfo)))
	for _, err := range []error{workspaceStoreError(database.ErrConflict), workspaceAdmissionError(errors.New("unknown database outcome"))} {
		for _, detail := range status.Convert(err).Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok {
				require.NotEqual(t, "WORKSPACE_ADMISSION_REJECTED", info.Reason)
			}
		}
	}
}

type cancelAfterGet struct {
	*lifecycleTestStore
	cancel context.CancelFunc
}

func (s *cancelAfterGet) GetSession(ctx context.Context, id, creator string) (*apiv1.Session, error) {
	session, err := s.Client.GetSession(ctx, id, creator)
	s.cancel()
	return session, err
}

func TestWorkspaceCapacityWaitNeverReservesSession(t *testing.T) {
	store, workflow, request, ctx := workspaceFixture(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sessions := NewService(&cancelAfterGet{store, cancel}, serviceTestAuthorizer{}, workflow)
	service := NewWorkspaceService(store, sessions, workflow, nil, time.Second)
	require.NoError(t, service.downloads.Acquire(t.Context(), 4*maxWorkspaceFileBytes))
	defer service.downloads.Release(4 * maxWorkspaceFileBytes)
	err := service.ReadFiles(ctx, batchRequest(request), func(*apiv1.ReadWorkspaceFilesResponse) error { t.Fatal("over capacity"); return nil })
	require.ErrorIs(t, err, context.Canceled)
	work, err := store.BeginWorkspaceAccess(t.Context(), request.SessionId, request.ExpectedHeadTaskId, request.Path, time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.ReleaseWorkspacePreparation(t.Context(), work))
}

type rejectedBatchAdvance struct{ *lifecycleTestStore }

func (s *rejectedBatchAdvance) NextWorkspaceRead(context.Context, *database.WorkspaceAccess, string) (*database.WorkspaceAccess, error) {
	return nil, database.ErrConflict
}

func TestWorkspaceRejectedAdvanceCleansUpWithoutAnotherRead(t *testing.T) {
	store, workflow, request, ctx := workspaceFixture(t)
	actors := &batchActors{lifecycleTestActors: workflow.actors.(*lifecycleTestActors)}
	workflow.actors = actors
	reads := 0
	reader := workspaceReadFunc(func(context.Context, *database.WorkspaceAccess, io.Writer) (bool, error) {
		reads++
		return true, nil
	})
	service := NewWorkspaceService(&rejectedBatchAdvance{store}, NewService(store, serviceTestAuthorizer{}, workflow), workflow, reader, time.Second)
	err := service.ReadFiles(ctx, batchRequest(request), func(*apiv1.ReadWorkspaceFilesResponse) error { t.Fatal("rejected batch exposed bytes"); return nil })
	require.Error(t, err)
	require.Equal(t, 1, reads)
	require.Equal(t, 1, actors.suspends)
	require.NoError(t, store.ReserveSessionDispatch(t.Context(), request.SessionId, uuid.New(), "next"))
}
