package grpcserver

import (
	"context"
	"io"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	runtimev1 "github.com/kagent-dev/kagent/go/api/gen/kagent/runtime/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/internal/jwttest"
	"github.com/kagent-dev/kagent/go/core/internal/runtimeauth"
	sessionsvc "github.com/kagent-dev/kagent/go/core/internal/service/session"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

type workspaceRPCActors struct{ substrate.LifecycleClient }

func (*workspaceRPCActors) GetActor(_ context.Context, space, name string) (*ateapipb.Actor, error) {
	return &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: space, Name: name, Uid: "actor-uid"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: space, Name: "assistant-kagent-revision"}}, nil
}

func (a *workspaceRPCActors) SuspendActor(ctx context.Context, space, name string) (*ateapipb.Actor, error) {
	actor, err := a.GetActor(ctx, space, name)
	actor.Status = &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		Snapshots: []*ateapipb.Snapshot{{Generation: 1, Fidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			DurableSnapshot: &ateapipb.SnapshotStorage{Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
				Object: &ateapipb.ObjectSnapshot{SnapshotUri: "s3://workspace-rpc-snapshot"}}}}}
	return actor, err
}

type workspaceRPCRead func(context.Context, *database.WorkspaceAccess, io.Writer) (bool, error)

func (f workspaceRPCRead) Read(ctx context.Context, work *database.WorkspaceAccess, writer io.Writer) (bool, error) {
	return f(ctx, work, writer)
}

func TestWorkspacePublicAndActorAuthorizationThroughGRPC(t *testing.T) {
	dsn := dbtest.StartT(context.WithoutCancel(t.Context()), t)
	dbtest.MigrateT(t, dsn, false)
	db, err := database.Connect(t.Context(), &database.PostgresConfig{Pool: dbtest.PoolConfig(dsn)})
	require.NoError(t, err)
	t.Cleanup(db.Close)
	store := database.NewClient(db, "public")
	session := createTaskStoreSession(t, store)
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("build"))
	message.ContextID = session.ContextId
	task := a2a.NewSubmittedTask(message, message)
	task.Status.State = a2a.TaskStateCompleted
	version, err := store.CreateRuntimeTask(t.Context(), session.Id, make([]byte, 32), task, "")
	require.NoError(t, err)
	require.NoError(t, store.SettleSessionTask(t.Context(), session.Id, string(task.ID), version))
	issuer := jwttest.New(t)
	verifier, err := substrate.NewActorJWTVerifier(t.Context(), issuer.URL, issuer.CAFile, substrate.RuntimeJWTAudience)
	require.NoError(t, err)
	actorToken := issuer.Token(t, issuer.Claims("team-a", substrate.ActorName(session.Id), "actor-uid"))
	wrongActor := issuer.Token(t, issuer.Claims("team-a", substrate.ActorName(session.Id), "replaced-uid"))
	policy := runtimeauth.NewAuthorizer(&auth.NoopAuthorizer{}, store)
	workflow := sessionsvc.NewActorWorkflow(store, &workspaceRPCActors{}, make(chan struct{}, 1), time.Hour)
	sessions := sessionsvc.NewService(store, policy, workflow)
	var access runtimev1.WorkspaceAccessServiceClient
	reader := workspaceRPCRead(func(ctx context.Context, work *database.WorkspaceAccess, output io.Writer) (bool, error) {
		request := &runtimev1.AuthorizeReadRequest{GrantId: work.GrantID.String(), Path: work.Path}
		for _, token := range []string{"", wrongActor} {
			_, err := access.AuthorizeRead(metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token)), request)
			if err == nil {
				return true, status.Error(codes.Internal, "unauthorized actor consumed grant")
			}
		}
		actorCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+actorToken))
		_, err := access.AuthorizeRead(actorCtx, request)
		if err != nil {
			return true, err
		}
		_, err = access.AuthorizeRead(actorCtx, request)
		if status.Code(err) != codes.PermissionDenied {
			return true, status.Error(codes.Internal, "grant reused")
		}
		_, err = output.Write([]byte("glb-bytes"))
		return true, err
	})
	workspaces := sessionsvc.NewWorkspaceService(store, sessions, workflow, reader, time.Second)
	connection := newConfigurationConnection(t, nil, func(config *Config) {
		config.Authenticator = runtimeauth.NewProvider(&authimpl.InsecureAuthenticator{}, verifier, store, issuer.URL)
		config.SessionService, config.WorkspaceService, config.ShareStore = sessions, workspaces, store
	})
	access = runtimev1.NewWorkspaceAccessServiceClient(connection)
	client := apiv1.NewSessionServiceClient(connection)
	owner := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-user-id", "alice", "ate-target-actor", "attacker/other"))
	share, err := client.CreateSessionShare(owner, &apiv1.CreateSessionShareRequest{SessionId: session.Id})
	require.NoError(t, err)
	request := &apiv1.ReadWorkspaceFileRequest{SessionId: session.Id, ExpectedHeadTaskId: string(task.ID), Path: "result.glb"}
	for _, denied := range []struct {
		name string
		ctx  context.Context
		code codes.Code
	}{
		{"other owner", metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-user-id", "bob")), codes.NotFound},
		{"share", metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-user-id", "bob", "x-share-token", share.Token)), codes.PermissionDenied},
		{"runtime", metadata.NewOutgoingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+actorToken)), codes.PermissionDenied},
	} {
		t.Run(denied.name, func(t *testing.T) {
			stream, err := client.ReadWorkspaceFile(denied.ctx, request)
			require.NoError(t, err)
			_, err = stream.Recv()
			require.Equal(t, denied.code, status.Code(err))
			batch, err := client.ReadWorkspaceFiles(denied.ctx, &apiv1.ReadWorkspaceFilesRequest{SessionId: request.SessionId, Paths: []string{request.Path}, ExpectedHeadTaskId: request.ExpectedHeadTaskId})
			require.NoError(t, err)
			_, err = batch.Recv()
			require.Equal(t, denied.code, status.Code(err))
		})
	}
	stream, err := client.ReadWorkspaceFile(owner, request)
	require.NoError(t, err)
	chunk, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte("glb-bytes"), chunk.Chunk)
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	batch, err := client.ReadWorkspaceFiles(owner, &apiv1.ReadWorkspaceFilesRequest{SessionId: request.SessionId, Paths: []string{"result.glb", "result.blend"}, ExpectedHeadTaskId: request.ExpectedHeadTaskId})
	require.NoError(t, err)
	for _, path := range []string{"result.glb", "result.blend"} {
		response, err := batch.Recv()
		require.NoError(t, err)
		require.Equal(t, path, response.Path)
		require.Equal(t, []byte("glb-bytes"), response.Chunk)
	}
	_, err = batch.Recv()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, store.ReserveSessionDispatch(t.Context(), session.Id, uuid.New(), "next"))
}

func TestWorkspaceRequestValidationAndPolicies(t *testing.T) {
	validator, err := protovalidate.New()
	require.NoError(t, err)
	id := uuid.NewString()
	for _, path := range []string{"", "/result.glb", "../secret", "a/../b", ".", "a/.", "a//b", "a/", "a\\b", "a\x00b"} {
		for _, request := range []proto.Message{
			&apiv1.ReadWorkspaceFileRequest{SessionId: id, ExpectedHeadTaskId: "task", Path: path},
			&apiv1.ReadWorkspaceFilesRequest{SessionId: id, ExpectedHeadTaskId: "task", Paths: []string{path}},
			&runtimev1.ReadFileRequest{GrantId: id, Path: path},
		} {
			require.Error(t, validator.Validate(request), "path %q", path)
		}
	}
	require.NoError(t, validator.Validate(&apiv1.ReadWorkspaceFileRequest{SessionId: id, ExpectedHeadTaskId: "task", Path: "output/model.glb"}))
	require.NoError(t, validator.Validate(&runtimev1.ReadFileRequest{GrantId: id, Path: "output/model.glb"}))
	require.Error(t, validator.Validate(&runtimev1.AuthorizeReadRequest{GrantId: "bad", Path: "result.glb"}))
	for _, paths := range [][]string{nil, {"x", "x"}, {"a", "b", "c", "d", "e"}} {
		require.Error(t, validator.Validate(&apiv1.ReadWorkspaceFilesRequest{SessionId: id, ExpectedHeadTaskId: "task", Paths: paths}))
	}
	require.NoError(t, validator.Validate(&apiv1.ReadWorkspaceFilesRequest{SessionId: id, ExpectedHeadTaskId: "task", Paths: []string{"result.glb", "output/model.blend"}}))
	policies := DefaultMethodPolicies()
	require.Equal(t, auth.AccessUpdate, policies[apiv1.SessionService_ReadWorkspaceFiles_FullMethodName])
	require.Equal(t, auth.AccessUpdate, policies[apiv1.SessionService_ReadWorkspaceFile_FullMethodName])
	require.Equal(t, auth.AccessRuntime, policies[runtimev1.WorkspaceAccessService_AuthorizeRead_FullMethodName])
	_, public := policies[runtimev1.WorkspaceService_ReadFile_FullMethodName]
	require.False(t, public, "private runtime service must not be publicly forwarded")
}
