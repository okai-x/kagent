package a2agateway

import (
	"context"
	"net"
	"testing"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

func TestMachineRuntimeDialerDoesNotForwardBearer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	received := make(chan metadata.MD, 1)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, _ any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		received <- md
		return nil, status.Error(codes.Unimplemented, "routing observed")
	}))
	a2apb.RegisterA2AServiceServer(server, &a2apb.UnimplementedA2AServiceServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	dialer, err := NewRuntimeDialer("http://"+listener.Addr().String(), &authimpl.RejectUserAuthenticator{})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = auth.AuthSessionTo(ctx, auth.NewCredentialSession(auth.Principal{ID: "verified-owner"}, machineTokenSource{}))
	client, err := dialer.Dial(ctx, &apiv1.Session{
		Id: "session", A2AAuthority: substrate.ActorHost("team", "session-session", ""),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Destroy()) })
	_, err = client.SendMessage(ctx, &a2atype.SendMessageRequest{
		Message: a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart("hello")),
	})
	require.Error(t, err)
	select {
	case md := <-received:
		require.Empty(t, md.Get("authorization"))
		require.Equal(t, []string{"verified-owner"}, md.Get("x-user-id"))
		require.Equal(t, []string{"team/session-session"}, md.Get("ate-target-actor"))
	case <-ctx.Done():
		t.Fatal("runtime did not receive the call")
	}
}

type machineTokenSource struct{}

func (machineTokenSource) Token(context.Context) (string, error) {
	return "service-account-test-credential", nil
}
