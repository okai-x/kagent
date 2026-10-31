package session

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/runtimeauth"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

type workspaceStore interface {
	GetSessionForRuntime(context.Context, string, string) (*apiv1.Session, error)
	BeginWorkspaceAccess(context.Context, string, string, string, time.Duration) (*database.WorkspaceAccess, error)
	IssueWorkspaceRead(context.Context, *database.WorkspaceAccess) error
	ConsumeWorkspaceGrant(context.Context, string, string, string) (time.Time, error)
	ReleaseWorkspacePreparation(context.Context, *database.WorkspaceAccess) error
	CompleteWorkspaceRead(context.Context, *database.WorkspaceAccess) error
	NextWorkspaceRead(context.Context, *database.WorkspaceAccess, string) (*database.WorkspaceAccess, error)
	IssueWorkspaceQuiescence(context.Context, *database.WorkspaceAccess, int32) error
	FinishWorkspaceAccess(context.Context, *database.WorkspaceAccess, *database.SessionTaskSnapshot) error
	RecoverWorkspacePreparation(context.Context) error
	ClaimWorkspaceCleanup(context.Context) (*database.WorkspaceAccess, error)
}

type workspaceReader interface {
	Read(context.Context, *database.WorkspaceAccess, io.Writer) (bool, error)
}

// WorkspaceService coordinates downloads with execution and actor lifecycle.
// It is intentionally separate from the model's local file and shell tools.
type WorkspaceService struct {
	store            workspaceStore
	sessions         *Service
	workflow         *ActorWorkflow
	reader           workspaceReader
	recoveryInterval time.Duration
	downloads        *semaphore.Weighted
}

func NewWorkspaceService(store workspaceStore, sessions *Service, workflow *ActorWorkflow, reader workspaceReader, interval time.Duration) *WorkspaceService {
	return &WorkspaceService{store: store, sessions: sessions, workflow: workflow, reader: reader, recoveryInterval: interval,
		downloads: semaphore.NewWeighted(4 * maxWorkspaceFileBytes)}
}

// Read returns bytes only after the definite runtime read, suspension and
// snapshot record all succeed. Client cancellation does not abandon cleanup.
func (s *WorkspaceService) Read(ctx context.Context, request *apiv1.ReadWorkspaceFileRequest, send func([]byte) error) error {
	batch := &apiv1.ReadWorkspaceFilesRequest{SessionId: request.SessionId, Paths: []string{request.Path}, ExpectedHeadTaskId: request.ExpectedHeadTaskId}
	return s.ReadFiles(ctx, batch, func(result *apiv1.ReadWorkspaceFilesResponse) error {
		if result.Missing {
			return workspaceReadError(status.Error(codes.NotFound, "missing"))
		}
		if len(result.Chunk) == 0 {
			return nil
		}
		return send(result.Chunk)
	})
}

func workspaceCaller(ctx context.Context) error {
	identity, ok := auth.AuthSessionFrom(ctx)
	if !ok {
		return serviceerrors.NewUnauthenticated("Workspace access requires authentication", nil)
	}
	_, shared := auth.ShareContextFrom(ctx)
	if shared || identity.Principal().RuntimeBinding != nil {
		return serviceerrors.NewPermissionDenied("Workspace downloads require the session owner", nil)
	}
	return nil
}

func (s *WorkspaceService) releasePreparation(ctx context.Context, work *database.WorkspaceAccess, cause error) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return serviceerrors.NewUnavailable("Workspace download preparation failed", errors.Join(cause, s.store.ReleaseWorkspacePreparation(releaseCtx, work)))
}

// AuthorizeRead consumes a grant exactly once using a verified Actor JWT and
// the recorded immutable Actor binding, never caller-provided routing metadata.
func (s *WorkspaceService) AuthorizeRead(ctx context.Context, grantID, path string) (time.Time, error) {
	if uuid.Validate(grantID) != nil || !validWorkspacePath(path) {
		return time.Time{}, status.Error(codes.InvalidArgument, "Invalid workspace grant request")
	}
	session, err := runtimeauth.Session(ctx, s.store)
	if err != nil {
		return time.Time{}, err
	}
	expires, err := s.store.ConsumeWorkspaceGrant(ctx, session.Id, grantID, path)
	if errors.Is(err, database.ErrNotFound) {
		return time.Time{}, status.Error(codes.PermissionDenied, "Workspace grant is not available")
	}
	if err != nil {
		return time.Time{}, serviceerrors.NewUnavailable("Workspace authorization unavailable", err)
	}
	return expires, nil
}

func validWorkspacePath(path string) bool {
	if path == "" || len(path) > 4096 || !utf8.ValidString(path) || strings.ContainsAny(path, "\x00\\") {
		return false
	}
	for part := range strings.SplitSeq(path, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func sendWorkspaceFile(ctx context.Context, file io.Reader, send func([]byte) error) error {
	buffer := make([]byte, 64<<10)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := file.Read(buffer)
		if n > 0 {
			if sendErr := send(buffer[:n]); sendErr != nil {
				return sendErr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return serviceerrors.NewInternal("Failed to read collected workspace file", err)
		}
	}
}

func workspaceStoreError(err error) error {
	if errors.Is(err, database.ErrConflict) || errors.Is(err, database.ErrFailedPrecondition) {
		return serviceerrors.NewFailedPrecondition("Workspace is not at the requested idle task boundary", err)
	}
	if errors.Is(err, database.ErrNotFound) {
		return serviceerrors.NewNotFound("Session not found", err)
	}
	return serviceerrors.NewUnavailable("Workspace access unavailable", err)
}

func workspaceReadError(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		result, detailsErr := status.New(codes.NotFound, "Workspace file not found").WithDetails(&errdetails.ErrorInfo{Reason: "WORKSPACE_FILE_NOT_FOUND", Domain: "kagent.dev"})
		if detailsErr != nil {
			return serviceerrors.NewInternal("Failed to encode workspace error", detailsErr)
		}
		return result.Err()
	case codes.Unimplemented:
		return serviceerrors.NewFailedPrecondition("Session runtime does not support workspace downloads", err)
	case codes.InvalidArgument, codes.FailedPrecondition, codes.ResourceExhausted, codes.PermissionDenied, codes.Aborted:
		return status.Error(status.Code(err), "Workspace file cannot be downloaded")
	default:
		return serviceerrors.NewUnavailable("Workspace download failed", err)
	}
}
