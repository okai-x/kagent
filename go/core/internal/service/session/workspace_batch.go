package session

import (
	"context"
	"errors"
	"io"
	"os"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

type workspaceFile struct {
	path    string
	file    *os.File
	missing bool
}

// ReadFiles retains one task boundary and suspends once after all definite reads.
// No public bytes escape until cleanup commits. An uncertain read or grant
// transition retains admission fencing and is never replayed by this service.
func (s *WorkspaceService) ReadFiles(ctx context.Context, request *apiv1.ReadWorkspaceFilesRequest, send func(*apiv1.ReadWorkspaceFilesResponse) error) (err error) {
	if !validWorkspacePaths(request.Paths) || request.ExpectedHeadTaskId == "" {
		return serviceerrors.NewInvalidArgument("One to four distinct canonical paths and a terminal task ID are required", nil)
	}
	if err := workspaceCaller(ctx); err != nil {
		return err
	}
	session, err := s.sessions.getAuthorized(ctx, request.SessionId, auth.VerbUpdate)
	if err != nil {
		return err
	}
	// Reserve disk capacity before admission, shared with single-file downloads.
	capacity := int64(len(request.Paths) * maxWorkspaceFileBytes)
	if err := s.downloads.Acquire(ctx, capacity); err != nil {
		return err
	}
	defer s.downloads.Release(capacity)
	files, err := createWorkspaceFiles(request.Paths)
	defer func() { err = errors.Join(err, closeWorkspaceFiles(files)) }()
	if err != nil {
		return serviceerrors.NewInternal("Workspace download could not be prepared", err)
	}
	work, err := s.store.BeginWorkspaceAccess(ctx, session.Id, request.ExpectedHeadTaskId, request.Paths[0], database.RuntimeOperationTimeout)
	if err != nil {
		return workspaceAdmissionError(err)
	}
	if _, _, err := s.workflow.prepareQuiescence(ctx, session); err != nil {
		return s.releasePreparation(ctx, work, err)
	}
	if err := s.store.IssueWorkspaceRead(ctx, work); err != nil {
		return workspaceStoreError(err)
	}
	work, definite, readErr := s.collectWorkspaceFiles(ctx, work, files)
	if !definite {
		return readErr
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), database.RuntimeOperationTimeout)
	defer cancel()
	if err := s.cleanup(cleanupCtx, work); err != nil {
		return err
	}
	if readErr != nil {
		return readErr
	}
	return sendWorkspaceFiles(ctx, files, send)
}

func (s *WorkspaceService) collectWorkspaceFiles(ctx context.Context, work *database.WorkspaceAccess, files []workspaceFile) (*database.WorkspaceAccess, bool, error) {
	for i := range files {
		if i > 0 {
			if err := ctx.Err(); err != nil {
				return work, true, err
			}
			next, err := s.store.NextWorkspaceRead(ctx, work, files[i].path)
			if err != nil {
				return work, errors.Is(err, database.ErrConflict), workspaceStoreError(err)
			}
			work = next
		}
		readCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), work.ExpiresAt)
		definite, readErr := s.reader.Read(readCtx, work, files[i].file)
		cancel()
		if !definite {
			return work, false, serviceerrors.NewUnavailable("Workspace read outcome is unresolved; session access remains blocked", readErr)
		}
		completeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), database.RuntimeOperationTimeout)
		err := s.store.CompleteWorkspaceRead(completeCtx, work)
		cancel()
		if err != nil {
			return work, false, workspaceStoreError(err)
		}
		if readErr != nil && status.Code(readErr) != codes.NotFound {
			return work, true, workspaceReadError(readErr)
		}
		if readErr != nil {
			info, err := files[i].file.Stat()
			if err != nil || info.Size() != 0 {
				return work, true, serviceerrors.NewUnavailable("Workspace returned an incomplete file response", err)
			}
		}
		files[i].missing = readErr != nil
	}
	return work, true, ctx.Err()
}

func validWorkspacePaths(paths []string) bool {
	if len(paths) < 1 || len(paths) > 4 {
		return false
	}
	seen := make(map[string]bool, len(paths))
	for _, path := range paths {
		if !validWorkspacePath(path) || seen[path] {
			return false
		}
		seen[path] = true
	}
	return true
}

func createWorkspaceFiles(paths []string) ([]workspaceFile, error) {
	files := make([]workspaceFile, 0, len(paths))
	for _, path := range paths {
		file, err := os.CreateTemp("", "kagent-workspace-*")
		if err != nil {
			return files, err
		}
		files = append(files, workspaceFile{path: path, file: file})
	}
	return files, nil
}

func closeWorkspaceFiles(files []workspaceFile) (err error) {
	for _, file := range files {
		err = errors.Join(err, file.file.Close(), os.Remove(file.file.Name()))
	}
	return err
}

func sendWorkspaceFiles(ctx context.Context, files []workspaceFile, send func(*apiv1.ReadWorkspaceFilesResponse) error) error {
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if file.missing {
			if err := send(&apiv1.ReadWorkspaceFilesResponse{Path: file.path, Missing: true}); err != nil {
				return err
			}
			continue
		}
		size, err := file.file.Seek(0, io.SeekEnd)
		if err != nil {
			return serviceerrors.NewInternal("Failed to read collected workspace file", err)
		}
		if _, err := file.file.Seek(0, io.SeekStart); err != nil {
			return serviceerrors.NewInternal("Failed to read collected workspace file", err)
		}
		if size == 0 {
			if err := send(&apiv1.ReadWorkspaceFilesResponse{Path: file.path}); err != nil {
				return err
			}
		}
		if err := sendWorkspaceFile(ctx, file.file, func(chunk []byte) error {
			return send(&apiv1.ReadWorkspaceFilesResponse{Path: file.path, Chunk: chunk})
		}); err != nil {
			return err
		}
	}
	return nil
}

// Only a rejected admission has this detail: no private read or suspension has
// been issued by this call. Clients may retry this gate for a bounded period.
func workspaceAdmissionError(err error) error {
	if errors.Is(err, database.ErrFailedPrecondition) || errors.Is(err, database.ErrConflict) {
		result, detailErr := status.New(codes.FailedPrecondition, "Workspace is not at the requested idle task boundary").WithDetails(
			&errdetails.ErrorInfo{Reason: "WORKSPACE_ADMISSION_REJECTED", Domain: "kagent.dev"})
		if detailErr != nil {
			return serviceerrors.NewInternal("Failed to encode workspace error", detailErr)
		}
		return result.Err()
	}
	return workspaceStoreError(err)
}
