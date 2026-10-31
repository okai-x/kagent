package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

const (
	historyPageBytes  = 2 << 20
	historyEventBytes = 8 << 20
)

var ErrHistoryEventTooLarge = errors.New("history event exceeds export size limit")

type historyEventSize struct {
	Sequence int64
	Bytes    int64
}

// ExportSessionHistory reads an immutable native history prefix, scoped to the
// authorized session owner. Every page uses a short read-only snapshot, never
// holds row locks, and returns exact event bytes rather than decoded projections.
// The session must still exist and match the cutoff's history. Missing/foreign
// sessions return ErrNotFound; stale or impossible boundaries fail explicitly.
// Runtime writers serialize on the session before allocating event sequences,
// so later commits cannot insert an event below a captured committed head.
func (c *Client) ExportSessionHistory(ctx context.Context, owner string, request *apiv1.ExportSessionHistoryRequest) (*apiv1.ExportSessionHistoryResponse, error) {
	if request.PageSize < 1 || request.PageSize > 100 || request.AfterSequence < 0 {
		return nil, ErrFailedPrecondition
	}
	var result *apiv1.ExportSessionHistoryResponse
	err := pgx.BeginTxFunc(ctx, c.db, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var err error
		result, err = exportHistoryPage(ctx, tx, owner, request)
		return err
	})
	return result, err
}

// exportHistoryPage resolves the boundary and payloads in one read-only snapshot.
func exportHistoryPage(ctx context.Context, tx pgx.Tx, owner string, request *apiv1.ExportSessionHistoryRequest) (*apiv1.ExportSessionHistoryResponse, error) {
	cutoff, err := readHistoryCutoff(ctx, tx, request.SessionId, owner)
	if err != nil {
		return nil, err
	}
	if supplied := request.Cutoff; supplied != nil {
		if supplied.HistoryId != cutoff.HistoryId || supplied.ThroughSequence < 0 || supplied.ThroughSequence > cutoff.ThroughSequence {
			return nil, ErrFailedPrecondition
		}
		cutoff = supplied
	} else if request.AfterSequence != 0 {
		return nil, ErrFailedPrecondition
	}
	if request.AfterSequence > cutoff.ThroughSequence {
		return nil, ErrFailedPrecondition
	}
	sizes, err := queryMany(ctx, tx, `
		SELECT sequence, octet_length(data)::bigint + octet_length(task_id)
		    + COALESCE(octet_length(message_id), 0) + 64 AS bytes
		FROM session_task_event
		WHERE history_id = $1 AND sequence > $2 AND sequence <= $3
		ORDER BY sequence LIMIT $4
	`, pgx.RowToStructByName[historyEventSize], cutoff.HistoryId, request.AfterSequence, cutoff.ThroughSequence, request.PageSize+1)
	if err != nil {
		return nil, fmt.Errorf("failed to size history page: %w", err)
	}
	sequences, err := selectHistoryPage(sizes, int(request.PageSize))
	if err != nil {
		return nil, err
	}
	events, err := readHistoryPage(ctx, tx, cutoff.HistoryId, sequences)
	if err != nil {
		return nil, err
	}
	result := &apiv1.ExportSessionHistoryResponse{Cutoff: cutoff, Events: events,
		NextAfterSequence: request.AfterSequence, HasMore: len(sizes) > len(events)}
	if len(events) > 0 {
		result.NextAfterSequence = events[len(events)-1].Sequence
	}
	return result, nil
}

// readHistoryCutoff binds ownership and the committed head in the same snapshot.
func readHistoryCutoff(ctx context.Context, db dbExecutor, sessionID, owner string) (*apiv1.SessionHistoryCutoff, error) {
	type head struct {
		HistoryID uuid.UUID
		Sequence  int64
	}
	row, err := queryOne(ctx, db, `
		SELECT s.history_id, COALESCE((SELECT MAX(e.sequence)
		    FROM session_task_event e WHERE e.history_id = s.history_id), 0) AS sequence
		FROM session_record s WHERE s.id = $1 AND s.user_id = $2
		    AND s.state <> 'RUNTIME_STATE_DELETED'
	`, pgx.RowToStructByName[head], sessionID, owner)
	if err != nil {
		return nil, fmt.Errorf("failed to read history boundary: %w", notFoundOr(err))
	}
	return &apiv1.SessionHistoryCutoff{HistoryId: row.HistoryID.String(), ThroughSequence: row.Sequence}, nil
}

func selectHistoryPage(sizes []historyEventSize, limit int) ([]int64, error) {
	var sequences []int64
	var total int64
	for _, size := range sizes {
		if len(sequences) == limit || (len(sequences) > 0 && total+size.Bytes > historyPageBytes) {
			break
		}
		if size.Bytes > historyEventBytes {
			return nil, ErrHistoryEventTooLarge
		}
		sequences = append(sequences, size.Sequence)
		total += size.Bytes
	}
	return sequences, nil
}

// readHistoryPage loads only the payloads selected by the bounded metadata scan.
// Callers must authorize the history and keep the same read-only transaction.
func readHistoryPage(ctx context.Context, db dbExecutor, historyID string, sequences []int64) ([]*apiv1.SessionHistoryEvent, error) {
	if len(sequences) == 0 {
		return nil, nil
	}
	type event struct {
		Sequence  int64
		TaskID    string
		MessageID *string
		CreatedAt time.Time
		Data      []byte
	}
	rows, err := queryMany(ctx, db, `
		SELECT sequence, task_id, message_id, created_at, data FROM session_task_event
		WHERE history_id = $1 AND sequence = ANY($2::bigint[]) ORDER BY sequence
	`, pgx.RowToStructByName[event], historyID, sequences)
	if err != nil {
		return nil, fmt.Errorf("failed to read history events: %w", err)
	}
	if len(rows) != len(sequences) {
		return nil, ErrFailedPrecondition
	}
	events := make([]*apiv1.SessionHistoryEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, &apiv1.SessionHistoryEvent{Sequence: row.Sequence,
			TaskId: row.TaskID, MessageId: row.MessageID,
			CreatedAt: timestamppb.New(row.CreatedAt), Data: row.Data})
	}
	return events, nil
}
