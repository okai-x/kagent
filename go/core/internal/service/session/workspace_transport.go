package session

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	runtimev1 "github.com/kagent-dev/kagent/go/api/gen/kagent/runtime/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

const maxWorkspaceFileBytes = 64 << 20

// WorkspaceDialer uses the existing Atenet Actor route, without A2A semantics.
// Only the controller's persisted binding selects its target and private grant.
type WorkspaceDialer struct {
	conn          *grpc.ClientConn
	authenticator auth.AuthProvider
	endpoint      string
}

func NewWorkspaceDialer(routerURL string, authenticator auth.AuthProvider) (*WorkspaceDialer, error) {
	router, err := url.Parse(routerURL)
	if err != nil || router.Host == "" || (router.Scheme != "http" && router.Scheme != "https") || authenticator == nil {
		return nil, fmt.Errorf("invalid workspace router configuration")
	}
	transport := insecure.NewCredentials()
	if router.Scheme == "https" {
		transport = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, ServerName: router.Hostname()})
	}
	conn, err := grpc.NewClient(router.Host, grpc.WithTransportCredentials(transport))
	if err != nil {
		return nil, err
	}
	return &WorkspaceDialer{conn: conn, authenticator: authenticator, endpoint: routerURL}, nil
}

func (d *WorkspaceDialer) Close() error { return d.conn.Close() }

func (d *WorkspaceDialer) context(ctx context.Context, work *database.WorkspaceAccess) (context.Context, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint, nil)
	if err != nil {
		return nil, err
	}
	identity, ok := auth.AuthSessionFrom(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "Workspace access requires authentication")
	}
	if err := d.authenticator.UpstreamAuth(request, identity, work.Session.Id); err != nil {
		return nil, err
	}
	target, err := substrate.ActorTargetFromHost(work.Session.A2AAuthority)
	if err != nil {
		return nil, err
	}
	md := metadata.MD{}
	for key, values := range request.Header {
		md[strings.ToLower(key)] = values
	}
	md.Set("ate-target-actor", target)
	return metadata.NewOutgoingContext(ctx, md), nil
}

// Read drains the private stream even if local spooling fails, so cleanup is
// issued only after a definite remote outcome. False means routing or execution
// may still be in flight and admission must remain closed across restarts.
func (d *WorkspaceDialer) Read(ctx context.Context, work *database.WorkspaceAccess, output io.Writer) (bool, error) {
	callCtx, err := d.context(ctx, work)
	if err != nil {
		return true, err
	}
	stream, err := runtimev1.NewWorkspaceServiceClient(d.conn).ReadFile(callCtx, &runtimev1.ReadFileRequest{
		GrantId: work.GrantID.String(), Path: work.Path,
	})
	if err != nil {
		return definiteWorkspaceOutcome(err), err
	}
	var total int
	var localErr error
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return true, localErr
		}
		if err != nil {
			if localErr != nil {
				return definiteWorkspaceOutcome(err), localErr
			}
			if status.Code(err) == codes.NotFound && total > 0 {
				return true, status.Error(codes.DataLoss, "Workspace returned an incomplete file response")
			}
			return definiteWorkspaceOutcome(err), err
		}
		total += len(chunk.Chunk)
		if total > maxWorkspaceFileBytes && localErr == nil {
			localErr = status.Error(codes.ResourceExhausted, "Workspace file exceeds 64 MiB")
		}
		if localErr == nil {
			_, localErr = output.Write(chunk.Chunk)
		}
	}
}

func definiteWorkspaceOutcome(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.FailedPrecondition, codes.ResourceExhausted, codes.PermissionDenied, codes.Unimplemented, codes.Aborted:
		return true
	default:
		return false
	}
}
