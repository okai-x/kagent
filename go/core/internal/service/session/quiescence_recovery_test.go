package session

import (
	"context"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
)

func TestQuiescenceExpiredPreparerCannotIssue(t *testing.T) {
	store, session := lifecycleFixture(t)
	base := &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}}
	workflow := NewActorWorkflow(store, base, make(chan struct{}, 1), time.Hour)
	session, err := workflow.Create(t.Context(), session)
	require.NoError(t, err)
	request := stageQuiescenceTask(t, store.Client, session, a2a.TaskStateCompleted)
	require.NoError(t, store.SettleSessionTask(t.Context(), session.Id, request.TaskId, request.Version))
	claimCtx, claimCancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer claimCancel()
	abandoned, err := store.ClaimSessionQuiescence(claimCtx)
	require.NoError(t, err)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	actors := &retryTestActors{lifecycleTestActors: base, beforeRead: func(ctx context.Context) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
	}}
	go func() {
		defer close(done)
		NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Hour).quiesceIdleSession(t.Context(), abandoned)
	}()
	<-entered
	var current *database.SessionQuiescence
	require.Eventually(t, func() bool {
		current, err = store.ClaimSessionQuiescence(t.Context())
		return err == nil
	}, time.Second, 10*time.Millisecond)
	close(release)
	<-done
	require.Zero(t, actors.mutations.Load(), "the old executor must be fenced before the runtime call")
	workflow.quiesceIdleSession(t.Context(), current)
	_, snapshot, err := store.ReserveSessionCheckpoint(t.Context(), &apiv1.Checkpoint{
		Id: uuid.NewString(), SessionId: session.Id, HeadTaskId: request.TaskId,
	}, "alice", uuid.NewString())
	require.NoError(t, err)
	require.Equal(t, "s3://snapshots/snapshot-1", snapshot.URI)
}

type quiescenceFailureActors struct {
	*retryTestActors
	beforeEffect bool
}

func (a *quiescenceFailureActors) SuspendActor(ctx context.Context, space, name string) (*ateapipb.Actor, error) {
	if a.beforeEffect {
		a.mutations.Add(1)
		return nil, status.Error(codes.Unavailable, "suspend result unknown")
	}
	return a.retryTestActors.SuspendActor(ctx, space, name)
}

type lostQuiescenceFinishStore struct {
	*lifecycleTestStore
	cancel context.CancelFunc
}

func (s *lostQuiescenceFinishStore) FinishSessionQuiescence(context.Context, *database.SessionQuiescence, *database.SessionTaskSnapshot) error {
	s.cancel()
	return status.Error(codes.Unavailable, "database unavailable before recording outcome")
}

func TestQuiescenceIssuedUncertaintyStaysClosedAcrossRestart(t *testing.T) {
	for _, test := range []struct {
		name         string
		beforeEffect bool
		lostFinish   bool
	}{
		{name: "request failed before effect", beforeEffect: true},
		{name: "response lost after effect"},
		{name: "snapshot completed but database outcome lost", lostFinish: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, session := lifecycleFixture(t)
			base := &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}}
			session, err := NewActorWorkflow(store, base, make(chan struct{}, 1), time.Hour).Create(t.Context(), session)
			require.NoError(t, err)
			_, err = base.ResumeActor(t.Context(), "team-a", substrate.ActorName(session.Id))
			require.NoError(t, err)
			request := stageQuiescenceTask(t, store.Client, session, a2a.TaskStateCompleted)
			require.NoError(t, store.SettleSessionTask(t.Context(), session.Id, request.TaskId, request.Version))
			work, err := store.ClaimSessionQuiescence(t.Context())
			require.NoError(t, err)
			actors := &quiescenceFailureActors{
				retryTestActors: &retryTestActors{lifecycleTestActors: base}, beforeEffect: test.beforeEffect,
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var writes workflowStore = store
			if test.lostFinish {
				writes = &lostQuiescenceFinishStore{lifecycleTestStore: store, cancel: cancel}
			} else {
				actors.mutationErr = status.Error(codes.Unavailable, "suspend result unknown")
			}
			NewActorWorkflow(writes, actors, make(chan struct{}, 1), time.Hour).quiesceIdleSession(ctx, work)
			require.EqualValues(t, 1, actors.mutations.Load())
			observed, err := base.GetActor(t.Context(), "team-a", substrate.ActorName(session.Id))
			require.NoError(t, err)
			if !test.beforeEffect {
				require.Equal(t, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, observed.GetStatus().GetState())
				require.NotEmpty(t, substrate.SnapshotURI(substrate.LatestActorSnapshot(observed)))
			}
			restartedActors := &retryTestActors{lifecycleTestActors: base}
			restartedStore := &observedQuiescenceStore{lifecycleTestStore: store}
			startQuiescenceWorker(t, NewActorWorkflow(restartedStore, restartedActors, make(chan struct{}, 1), time.Hour))
			require.Eventually(t, func() bool { return restartedStore.idle.Load() >= 4 }, 5*time.Second, 10*time.Millisecond)
			require.Zero(t, restartedActors.mutations.Load(), "restart must never replay an uncertain request")
			require.Zero(t, restartedStore.finished.Load(), "observed snapshot alone does not identify the issued operation")
			_, _, err = store.ReserveSessionCheckpoint(t.Context(), &apiv1.Checkpoint{
				Id: uuid.NewString(), SessionId: session.Id, HeadTaskId: request.TaskId,
			}, "alice", uuid.NewString())
			require.ErrorIs(t, err, database.ErrFailedPrecondition)
			next := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("next"))
			next.ContextID = session.ContextId
			_, err = store.CreateRuntimeTask(t.Context(), session.Id, make([]byte, 32), a2a.NewSubmittedTask(next, next), "")
			require.ErrorIs(t, err, database.ErrFailedPrecondition)
			visible, err := store.GetSettledSessionTask(t.Context(), session.Id, request.TaskId, nil)
			require.NoError(t, err)
			require.Equal(t, a2a.TaskStateCompleted, visible.Status.State)
		})
	}
}

// This contract fake demonstrates why observing the same UID and a complete
// snapshot cannot fence a delayed name-only SuspendActor request. It is not a
// fault-injection test against a live Substrate deployment.
func TestActorObservationCannotFenceLateSuspend(t *testing.T) {
	actors := &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}}
	_, err := actors.CreateActor(t.Context(), "team-a", "actor", "team-a", "template")
	require.NoError(t, err)
	_, err = actors.ResumeActor(t.Context(), "team-a", "actor")
	require.NoError(t, err)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		<-release
		_, err := actors.SuspendActor(t.Context(), "team-a", "actor")
		done <- err
	}()
	observed, err := actors.SuspendActor(t.Context(), "team-a", "actor")
	require.NoError(t, err)
	require.NotEmpty(t, substrate.SnapshotURI(substrate.LatestActorSnapshot(observed)))
	resumed, err := actors.ResumeActor(t.Context(), "team-a", "actor")
	require.NoError(t, err)
	require.Equal(t, observed.GetMetadata().GetUid(), resumed.GetMetadata().GetUid())
	close(release)
	require.NoError(t, <-done)
	current, err := actors.GetActor(t.Context(), "team-a", "actor")
	require.NoError(t, err)
	require.Equal(t, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, current.GetStatus().GetState(),
		"a late suspend can stop the new turn even though actor identity never changed")
}
