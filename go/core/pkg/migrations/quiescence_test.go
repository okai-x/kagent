package migrations

import (
	"database/sql"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestQuiescenceMigrationPreservesUnknownClaims(t *testing.T) {
	dsn, config := startTestDB(t)
	source := BuiltinSourcesInSchema(false, "public", "public")[0]
	if err := WithProvider(t.Context(), config, source, func(provider *goose.Provider) error {
		_, err := provider.UpTo(t.Context(), 1)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	const historyID = "00000000-0000-0000-0000-000000000001"
	const executorID = "00000000-0000-0000-0000-000000000002"
	execSQL(t, dsn, "INSERT INTO a2a_context (id, context_id) VALUES ($1, $1)", historyID)
	execSQL(t, dsn, `INSERT INTO session_task_event
		(history_id, task_id, data, quiescence_pending, quiescence_executor_id)
		VALUES ($1, 'task', $2, TRUE, $3)`, historyID, []byte{}, executorID)
	if err := RunUp(t.Context(), config, []Source{source}); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var executor, phase string
	var pending, missingEvidence bool
	if err := db.QueryRowContext(t.Context(), `SELECT quiescence_executor_id, quiescence_phase, quiescence_pending,
		quiescence_expires_at IS NULL AND quiescence_actor_uid IS NULL AND quiescence_snapshot_generation IS NULL
		FROM session_task_event WHERE history_id = $1`, historyID).Scan(&executor, &phase, &pending, &missingEvidence); err != nil {
		t.Fatal(err)
	}
	if executor != executorID || phase != "ISSUED" || !pending || !missingEvidence {
		t.Fatalf("migrated claim = %s/%s, pending=%t, missingEvidence=%t", executor, phase, pending, missingEvidence)
	}
	for _, statement := range []string{
		"UPDATE session_task_event SET quiescence_phase = NULL",
		"UPDATE session_task_event SET quiescence_phase = 'PREPARING'",
		"UPDATE session_task_event SET quiescence_actor_uid = 'partial-evidence'",
		"UPDATE session_task_event SET quiescence_snapshot_generation = 1",
	} {
		if _, err := db.ExecContext(t.Context(), statement); err == nil {
			t.Fatalf("invalid claim accepted: %s", statement)
		}
	}
	if err := WithProvider(t.Context(), config, source, func(provider *goose.Provider) error {
		_, err := provider.DownTo(t.Context(), 1)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), "SELECT quiescence_executor_id, quiescence_pending FROM session_task_event").Scan(&executor, &pending); err != nil {
		t.Fatal(err)
	}
	if executor != executorID || !pending {
		t.Fatal("down migration released an uncertain claim")
	}
}
