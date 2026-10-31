package grpcserver

import (
	"context"
	"testing"

	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	runtimev1 "github.com/kagent-dev/kagent/go/api/gen/kagent/runtime/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/internal/jwttest"
	"github.com/kagent-dev/kagent/go/core/internal/runtimeauth"
	sessionsvc "github.com/kagent-dev/kagent/go/core/internal/service/session"
	"github.com/kagent-dev/kagent/go/core/internal/service/taskstore"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

func TestSessionHistoryThroughGRPCAndPostgres(t *testing.T) {
	dsn := dbtest.StartT(context.WithoutCancel(t.Context()), t)
	dbtest.MigrateT(t, dsn, false)
	db, err := database.Connect(t.Context(), &database.PostgresConfig{Pool: dbtest.PoolConfig(dsn)})
	require.NoError(t, err)
	t.Cleanup(db.Close)
	store := database.NewClient(db, "public")
	session := createTaskStoreSession(t, store)
	issuer := jwttest.New(t)
	verifier, err := substrate.NewActorJWTVerifier(t.Context(), issuer.URL, issuer.CAFile, substrate.RuntimeJWTAudience)
	require.NoError(t, err)
	actorToken := issuer.Token(t, issuer.Claims("team-a", substrate.ActorName(session.Id), "actor-uid"))
	policy := runtimeauth.NewAuthorizer(auth.NoopAuthorizer{}, store)
	connection := newConfigurationConnection(t, nil, func(config *Config) {
		config.Authenticator = runtimeauth.NewProvider(&authimpl.InsecureAuthenticator{}, verifier, store, issuer.URL)
		config.SessionService = sessionsvc.NewService(store, policy, nil)
		config.TaskStoreService = taskstore.NewService(store, nil, make(chan struct{}, 1))
		config.ShareStore = store
	})
	client := apiv1.NewSessionServiceClient(connection)
	private := runtimev1.NewTaskStoreServiceClient(connection)
	owner := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-user-id", "alice"))
	actor := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+actorToken))
	request := &apiv1.ExportSessionHistoryRequest{SessionId: session.Id, PageSize: 1}
	empty, err := client.ExportSessionHistory(owner, request)
	require.NoError(t, err)
	require.Empty(t, empty.Events)
	message := &a2apb.Message{MessageId: "input", ContextId: session.ContextId, TaskId: "task", Role: a2apb.Role_ROLE_USER,
		Parts: []*a2apb.Part{{Content: &a2apb.Part_Text{Text: "Build model"}}}}
	task := &a2apb.Task{Id: "task", ContextId: session.ContextId, Status: &a2apb.TaskStatus{State: a2apb.TaskState_TASK_STATE_WORKING}, History: []*a2apb.Message{message}}
	unknown := protowire.AppendBytes(protowire.AppendTag(nil, 30000, protowire.BytesType), []byte("future-task-field"))
	created, err := private.CreateTask(actor, &runtimev1.TaskStoreServiceCreateTaskRequest{SessionId: session.Id, Task: task})
	require.NoError(t, err)
	first, err := client.ExportSessionHistory(owner, request)
	require.NoError(t, err)
	require.Len(t, first.Events, 1)
	require.True(t, first.HasMore)
	native := &a2apb.StreamResponse{}
	require.NoError(t, proto.Unmarshal(first.Events[0].Data, native))
	// Model bytes persisted by a newer producer. The existing TaskStore domain
	// conversion does not preserve unknown request fields before persistence.
	native.GetTask().ProtoReflect().SetUnknown(unknown)
	raw, err := proto.Marshal(native)
	require.NoError(t, err)
	_, err = db.Exec(t.Context(), `UPDATE session_task_event SET data = $2 WHERE sequence = $1`, first.Events[0].Sequence, raw)
	require.NoError(t, err)
	first, err = client.ExportSessionHistory(owner, request)
	require.NoError(t, err)
	require.Equal(t, raw, first.Events[0].Data)
	require.NoError(t, proto.Unmarshal(first.Events[0].Data, native))
	require.Equal(t, unknown, []byte(native.GetTask().ProtoReflect().GetUnknown()))
	request.Cutoff = first.Cutoff
	request.AfterSequence = first.NextAfterSequence
	last, err := client.ExportSessionHistory(owner, request)
	require.NoError(t, err)
	require.Len(t, last.Events, 1)
	require.Equal(t, "input", last.Events[0].GetMessageId())
	require.False(t, last.HasMore)
	task.Status.State = a2apb.TaskState_TASK_STATE_COMPLETED
	_, err = private.UpdateTask(actor, &runtimev1.TaskStoreServiceUpdateTaskRequest{SessionId: session.Id, Task: task, ExpectedVersion: created.Version})
	require.NoError(t, err)
	retried, err := client.ExportSessionHistory(owner, request)
	require.NoError(t, err)
	require.True(t, proto.Equal(last, retried), "later native appends must not change a retry")
	request.Cutoff, request.AfterSequence = empty.Cutoff, 0
	emptyAgain, err := client.ExportSessionHistory(owner, request)
	require.NoError(t, err)
	require.Empty(t, emptyAgain.Events)
	share, err := client.CreateSessionShare(owner, &apiv1.CreateSessionShareRequest{SessionId: session.Id})
	require.NoError(t, err)
	for _, denied := range []struct {
		name string
		ctx  context.Context
		code codes.Code
	}{
		{"other owner", metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-user-id", "bob")), codes.NotFound},
		{"share", metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-user-id", "bob", "x-share-token", share.Token)), codes.PermissionDenied},
		{"runtime", actor, codes.PermissionDenied},
	} {
		t.Run(denied.name, func(t *testing.T) {
			_, err := client.ExportSessionHistory(denied.ctx, request)
			require.Equal(t, denied.code, status.Code(err))
		})
	}
	_, err = client.ExportSessionHistory(owner, &apiv1.ExportSessionHistoryRequest{SessionId: uuid.NewString()})
	require.Equal(t, codes.NotFound, status.Code(err))
	for _, invalid := range []*apiv1.ExportSessionHistoryRequest{
		{SessionId: "bad"}, {SessionId: session.Id, PageSize: 101},
		{SessionId: session.Id, PageSize: -1}, {SessionId: session.Id, AfterSequence: -1},
		{SessionId: session.Id, AfterSequence: 1},
		{SessionId: session.Id, Cutoff: &apiv1.SessionHistoryCutoff{HistoryId: "bad"}},
		{SessionId: session.Id, AfterSequence: 1, Cutoff: empty.Cutoff},
	} {
		_, err = client.ExportSessionHistory(owner, invalid)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	require.Equal(t, auth.AccessRead, DefaultMethodPolicies()[apiv1.SessionService_ExportSessionHistory_FullMethodName])
}
