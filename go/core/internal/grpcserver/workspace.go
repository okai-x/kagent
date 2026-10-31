package grpcserver

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	runtimev1 "github.com/kagent-dev/kagent/go/api/gen/kagent/runtime/v1"
	sessionsvc "github.com/kagent-dev/kagent/go/core/internal/service/session"
)

func (s *sessionServer) ReadWorkspaceFile(request *apiv1.ReadWorkspaceFileRequest, stream grpc.ServerStreamingServer[apiv1.ReadWorkspaceFileResponse]) error {
	if s.workspace == nil {
		return status.Error(codes.Unavailable, "Workspace downloads are not configured")
	}
	return s.workspace.Read(stream.Context(), request, func(chunk []byte) error {
		return stream.Send(&apiv1.ReadWorkspaceFileResponse{Chunk: chunk})
	})
}

func (s *sessionServer) ReadWorkspaceFiles(request *apiv1.ReadWorkspaceFilesRequest, stream grpc.ServerStreamingServer[apiv1.ReadWorkspaceFilesResponse]) error {
	if s.workspace == nil {
		return status.Error(codes.Unavailable, "Workspace downloads are not configured")
	}
	return s.workspace.ReadFiles(stream.Context(), request, stream.Send)
}

type workspaceAccessServer struct {
	runtimev1.UnimplementedWorkspaceAccessServiceServer
	service *sessionsvc.WorkspaceService
}

func (s *workspaceAccessServer) AuthorizeRead(ctx context.Context, request *runtimev1.AuthorizeReadRequest) (*runtimev1.AuthorizeReadResponse, error) {
	if s.service == nil {
		return nil, status.Error(codes.Unavailable, "Workspace downloads are not configured")
	}
	expires, err := s.service.AuthorizeRead(ctx, request.GrantId, request.Path)
	if err != nil {
		return nil, err
	}
	return &runtimev1.AuthorizeReadResponse{ExpiresAt: timestamppb.New(expires)}, nil
}
