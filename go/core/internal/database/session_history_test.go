package database

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aevent"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

func historyFixture(t *testing.T) (*Client, *apiv1.Session, *a2a.Task) {
	t.Helper()
	client := NewClient(setupTestDB(t), "public")
	sessionFixture(t, client, t.Context(), "team-a", "revision", "assistant", "kagent")
	session, _, err := client.CreateSession(t.Context(), newSessionRequest(uuid.NewString(), "assistant", "kagent", "History"), uuid.NewString())
	require.NoError(t, err)
	_, err = markSessionReady(t.Context(), client, session.Id, "source.example")
	require.NoError(t, err)
	task := newSessionTask("task", "initial-message")
	task.ContextID = session.ContextId
	return client, session, task
}

func TestExportHistoryFrozenCutoffAndRawBytes(t *testing.T) {
	client, session, task := historyFixture(t)
	request := &apiv1.ExportSessionHistoryRequest{SessionId: session.Id, PageSize: 1}
	empty, err := client.ExportSessionHistory(t.Context(), session.Creator, request)
	require.NoError(t, err)
	require.Empty(t, empty.Events)
	require.Zero(t, empty.Cutoff.ThroughSequence)
	require.False(t, empty.HasMore)
	_, err = client.CreateRuntimeTask(t.Context(), session.Id, taskMutationHash("first"), task, "")
	require.NoError(t, err)
	first, err := client.ExportSessionHistory(t.Context(), session.Creator, request)
	require.NoError(t, err)
	require.Len(t, first.Events, 1)
	require.True(t, first.HasMore)
	require.NotZero(t, first.Events[0].CreatedAt.AsTime())
	require.Equal(t, string(task.ID), first.Events[0].TaskId)
	require.Nil(t, first.Events[0].MessageId)
	// Simulate a forward-version producer using the real durable event shape.
	raw := protowire.AppendBytes(protowire.AppendTag(bytes.Clone(first.Events[0].Data), 30000, protowire.BytesType), []byte("future-event-field"))
	_, err = client.db.Exec(t.Context(), `UPDATE session_task_event SET data = $2 WHERE sequence = $1`, first.Events[0].Sequence, raw)
	require.NoError(t, err)
	request.Cutoff = first.Cutoff
	replayed, err := client.ExportSessionHistory(t.Context(), session.Creator, request)
	require.NoError(t, err)
	require.Equal(t, raw, replayed.Events[0].Data)
	request.AfterSequence = replayed.NextAfterSequence
	last, err := client.ExportSessionHistory(t.Context(), session.Creator, request)
	require.NoError(t, err)
	require.Len(t, last.Events, 1)
	require.False(t, last.HasMore)
	require.Equal(t, "initial-message", last.Events[0].GetMessageId())
	request.AfterSequence = last.NextAfterSequence
	end, err := client.ExportSessionHistory(t.Context(), session.Creator, request)
	require.NoError(t, err)
	require.Empty(t, end.Events)
	require.False(t, end.HasMore)
	require.Equal(t, request.AfterSequence, end.NextAfterSequence)
	request.Cutoff, request.AfterSequence = empty.Cutoff, 0
	frozenEmpty, err := client.ExportSessionHistory(t.Context(), session.Creator, request)
	require.NoError(t, err)
	require.Empty(t, frozenEmpty.Events)
	require.False(t, frozenEmpty.HasMore)
}

func TestExportHistoryConcurrentAppendsDoNotChangeCutoff(t *testing.T) {
	client, session, task := historyFixture(t)
	_, err := client.CreateRuntimeTask(t.Context(), session.Id, taskMutationHash("first"), task, "")
	require.NoError(t, err)
	request := &apiv1.ExportSessionHistoryRequest{SessionId: session.Id, PageSize: 100}
	before, err := client.ExportSessionHistory(t.Context(), session.Creator, request)
	require.NoError(t, err)
	request.Cutoff = before.Cutoff
	written := make(chan error, 1)
	go func() {
		for i := range 20 {
			event := &a2a.TaskArtifactUpdateEvent{TaskID: task.ID, ContextID: task.ContextID,
				Artifact: &a2a.Artifact{ID: a2a.ArtifactID(fmt.Sprintf("artifact-%d", i)), Parts: a2a.ContentParts{a2a.NewTextPart("output")}}}
			version, err := taskVersion(t.Context(), client.db, uuid.MustParse(before.Cutoff.HistoryId), string(task.ID))
			if err == nil {
				task, err = a2aevent.ApplyUpdate(task, event)
			}
			if err == nil {
				_, err = client.UpdateSessionTask(t.Context(), session.Id, version, taskMutationHash(fmt.Sprintf("write-%d", i)), task, event, "")
			}
			if err != nil {
				written <- err
				return
			}
		}
		written <- nil
	}()
	for range 20 {
		after, err := client.ExportSessionHistory(t.Context(), session.Creator, request)
		require.NoError(t, err)
		require.True(t, proto.Equal(before, after))
	}
	require.NoError(t, <-written)
	request.Cutoff = nil
	after, err := client.ExportSessionHistory(t.Context(), session.Creator, request)
	require.NoError(t, err)
	require.Greater(t, after.Cutoff.ThroughSequence, before.Cutoff.ThroughSequence)
}

func TestExportHistoryByteBoundsAndOversizeFailure(t *testing.T) {
	client, session, task := historyFixture(t)
	_, err := client.CreateRuntimeTask(t.Context(), session.Id, taskMutationHash("first"), task, "")
	require.NoError(t, err)
	request := &apiv1.ExportSessionHistoryRequest{SessionId: session.Id, PageSize: 100}
	page, err := client.ExportSessionHistory(t.Context(), session.Creator, request)
	require.NoError(t, err)
	require.Len(t, page.Events, 2)
	// Deliberately large producer records verify sizing before bytea transfer.
	for _, event := range page.Events {
		_, err = client.db.Exec(t.Context(), `UPDATE session_task_event SET data = $2 WHERE sequence = $1`, event.Sequence, bytes.Repeat([]byte{1}, 1536<<10))
		require.NoError(t, err)
	}
	page, err = client.ExportSessionHistory(t.Context(), session.Creator, request)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	require.True(t, page.HasMore)
	require.Less(t, proto.Size(page), historyPageBytes)
	for _, size := range []int{4 << 20, 8 << 20} {
		_, err = client.db.Exec(t.Context(), `UPDATE session_task_event SET data = $2 WHERE sequence = $1`, page.Events[0].Sequence, bytes.Repeat([]byte{1}, size))
		require.NoError(t, err)
		result, err := client.ExportSessionHistory(t.Context(), session.Creator, request)
		if size == 8<<20 {
			require.ErrorIs(t, err, ErrHistoryEventTooLarge)
			require.Nil(t, result)
		} else {
			require.NoError(t, err)
			require.Len(t, result.Events, 1)
			require.True(t, result.HasMore)
			require.Less(t, proto.Size(result), 16<<20)
		}
	}
}

func TestExportHistoryScopeAndInvalidBoundary(t *testing.T) {
	client, session, task := historyFixture(t)
	_, err := client.CreateRuntimeTask(t.Context(), session.Id, taskMutationHash("first"), task, "")
	require.NoError(t, err)
	request := &apiv1.ExportSessionHistoryRequest{SessionId: session.Id, PageSize: 100}
	page, err := client.ExportSessionHistory(t.Context(), session.Creator, request)
	require.NoError(t, err)
	_, err = client.ExportSessionHistory(t.Context(), "not-the-owner", request)
	require.ErrorIs(t, err, ErrNotFound)
	for _, cutoff := range []*apiv1.SessionHistoryCutoff{
		{HistoryId: uuid.NewString(), ThroughSequence: page.Cutoff.ThroughSequence},
		{HistoryId: page.Cutoff.HistoryId, ThroughSequence: page.Cutoff.ThroughSequence + 1},
	} {
		request.Cutoff = cutoff
		_, err = client.ExportSessionHistory(t.Context(), session.Creator, request)
		require.ErrorIs(t, err, ErrFailedPrecondition)
	}
	request.SessionId = uuid.NewString()
	_, err = client.ExportSessionHistory(t.Context(), session.Creator, request)
	require.ErrorIs(t, err, ErrNotFound)
}
