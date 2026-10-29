package session

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	runtimev1 "github.com/kagent-dev/kagent/go/api/gen/kagent/runtime/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

type workspacePrivateRPC struct {
	runtimev1.UnimplementedWorkspaceServiceServer
	read func(*runtimev1.ReadFileRequest, grpc.ServerStreamingServer[runtimev1.ReadFileResponse]) error
}

func (s *workspacePrivateRPC) ReadFile(request *runtimev1.ReadFileRequest, stream grpc.ServerStreamingServer[runtimev1.ReadFileResponse]) error {
	return s.read(request, stream)
}

func TestWorkspaceDialerRebuildsRouteAndDrainsOversize(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "oversized"}[oversized], func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			received := make(chan metadata.MD, 1)
			requests := make(chan *runtimev1.ReadFileRequest, 1)
			server := grpc.NewServer()
			runtimev1.RegisterWorkspaceServiceServer(server, &workspacePrivateRPC{read: func(request *runtimev1.ReadFileRequest, stream grpc.ServerStreamingServer[runtimev1.ReadFileResponse]) error {
				md, _ := metadata.FromIncomingContext(stream.Context())
				received <- md
				requests <- request
				chunk, count := []byte("model"), 1
				if oversized {
					chunk, count = make([]byte, 1<<20), 65
				}
				for range count {
					if err := stream.Send(&runtimev1.ReadFileResponse{Chunk: chunk}); err != nil {
						return err
					}
				}
				return nil
			}})
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			dialer, err := NewWorkspaceDialer("http://"+listener.Addr().String(), &authimpl.InsecureAuthenticator{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dialer.Close()) })
			id := uuid.NewString()
			work := &database.WorkspaceAccess{Session: &apiv1.Session{Id: id, A2AAuthority: substrate.ActorHost("team-a", substrate.ActorName(id), "")}, GrantID: uuid.New(), Path: "result.glb"}
			ctx, cancel := context.WithTimeout(auth.AuthSessionTo(t.Context(), serviceTestSession{userID: "alice"}), 5*time.Second)
			defer cancel()
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("ate-target-actor", "attacker/other", "authorization", "forged", "x-user-id", "other", "kagent-sandbox-id", uuid.NewString()))
			var output bytes.Buffer
			var writer io.Writer = &output
			if oversized {
				writer = io.Discard
			}
			definite, err := dialer.Read(ctx, work, writer)
			require.True(t, definite, "oversize is drained through definite EOF before cleanup")
			if oversized {
				require.Equal(t, codes.ResourceExhausted, status.Code(err))
			} else {
				require.NoError(t, err)
				require.Equal(t, "model", output.String())
			}
			md := <-received
			require.Equal(t, []string{"team-a/" + substrate.ActorName(id)}, md.Get("ate-target-actor"))
			require.Equal(t, []string{"alice"}, md.Get("x-user-id"))
			require.Empty(t, md.Get("authorization"))
			require.Empty(t, md.Get("kagent-sandbox-id"))
			require.Equal(t, work.GrantID.String(), (<-requests).GrantId)
		})
	}
}

func TestWorkspaceAbortedReadCleansUpAndDiscardsPartialFile(t *testing.T) {
	store, workflow, request, ctx := workspaceFixture(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	runtimev1.RegisterWorkspaceServiceServer(server, &workspacePrivateRPC{read: func(_ *runtimev1.ReadFileRequest, stream grpc.ServerStreamingServer[runtimev1.ReadFileResponse]) error {
		if err := stream.Send(&runtimev1.ReadFileResponse{Chunk: []byte("partial-glb")}); err != nil {
			return err
		}
		return status.Error(codes.Aborted, "private workspace path changed")
	}})
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("serve private workspace RPC: %v", err)
		}
	}()
	t.Cleanup(server.Stop)
	dialer, err := NewWorkspaceDialer("http://"+listener.Addr().String(), &authimpl.InsecureAuthenticator{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dialer.Close()) })
	service := NewWorkspaceService(store, NewService(store, serviceTestAuthorizer{}, workflow), workflow, dialer, time.Second)
	err = service.Read(ctx, request, func([]byte) error {
		t.Fatal("failed download exposed partial file")
		return nil
	})
	require.Equal(t, codes.Aborted, status.Code(err))
	require.NotContains(t, err.Error(), "private workspace path")
	require.NoError(t, store.ReserveSessionDispatch(t.Context(), request.SessionId, uuid.New(), "next"), "definite runtime failure must release admission after cleanup")
}
