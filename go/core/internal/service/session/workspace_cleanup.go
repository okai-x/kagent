package session

import (
	"context"
	"errors"
	"time"

	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/pkg/logging"
)

func (s *WorkspaceService) cleanup(ctx context.Context, work *database.WorkspaceAccess) error {
	revision, actor, err := s.workflow.prepareQuiescence(ctx, work.Session)
	if err != nil {
		return serviceerrors.NewUnavailable("Workspace cleanup preparation failed", err)
	}
	if actor.GetMetadata().GetUid() != work.ActorUID {
		return serviceerrors.NewFailedPrecondition("Workspace Actor identity changed", nil)
	}
	if err := s.store.IssueWorkspaceQuiescence(ctx, work, substrate.LatestActorSnapshot(actor).GetGeneration()); err != nil {
		return workspaceStoreError(err)
	}
	snapshot, err := s.workflow.suspendQuiescence(ctx, revision, actor)
	if err != nil {
		return serviceerrors.NewUnavailable("Workspace cleanup outcome is unresolved; session access remains blocked", err)
	}
	return s.finish(ctx, work, snapshot)
}

func (s *WorkspaceService) finish(ctx context.Context, work *database.WorkspaceAccess, snapshot *database.SessionTaskSnapshot) error {
	for delay := 100 * time.Millisecond; ; delay = min(2*delay, 5*time.Second) {
		err := s.store.FinishWorkspaceAccess(ctx, work, snapshot)
		if err == nil {
			return nil
		}
		if errors.Is(err, database.ErrConflict) || errors.Is(err, database.ErrNotFound) {
			return workspaceStoreError(err)
		}
		logging.FromContext(ctx).ErrorContext(ctx, "record workspace snapshot", "session_id", work.Session.Id, "error", err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return serviceerrors.NewUnavailable("Workspace snapshot was not durably recorded", err)
		case <-timer.C:
		}
	}
}

func (*WorkspaceService) NeedLeaderElection() bool { return false }

// Start reclaims only phases proven to precede external effects. Issued reads
// and suspensions are deliberately retained when their outcome is uncertain.
func (s *WorkspaceService) Start(ctx context.Context) error {
	for ctx.Err() == nil {
		if err := s.recover(ctx); err != nil && ctx.Err() == nil {
			logging.FromContext(ctx).ErrorContext(ctx, "recover workspace access", "error", err)
		}
		timer := time.NewTimer(s.recoveryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return nil
}

func (s *WorkspaceService) recover(ctx context.Context) error {
	if err := s.store.RecoverWorkspacePreparation(ctx); err != nil {
		return err
	}
	for ctx.Err() == nil {
		work, err := s.store.ClaimWorkspaceCleanup(ctx)
		if errors.Is(err, database.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		cleanupCtx, cancel := context.WithTimeout(ctx, database.RuntimeOperationTimeout)
		err = s.cleanup(cleanupCtx, work)
		cancel()
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}
