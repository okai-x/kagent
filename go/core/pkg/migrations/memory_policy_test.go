package migrations

import (
	"database/sql"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

func TestRevisionMemoryPolicyMigrationDefaultsLegacyRevisionsToDenied(t *testing.T) {
	dsn, config := startTestDB(t)
	sources := BuiltinSourcesInSchema(false, "public", "public")
	require.NoError(t, WithProvider(t.Context(), config, sources[0], func(provider *goose.Provider) error {
		_, err := provider.UpTo(t.Context(), 4)
		return err
	}))
	execSQL(t, dsn, `INSERT INTO runtime_revision
		(revision, kind, namespace, source_snapshot, actor_template_atespace, actor_template_name)
		VALUES ('legacy', 'agent', 'team', '{}', 'team', 'legacy')`)
	execSQL(t, dsn, `INSERT INTO agent_revision (revision, agent_name, agent_uid, agent_card)
		VALUES ('legacy', 'assistant', 'agent-uid', $1)`, []byte{})
	require.NoError(t, RunUp(t.Context(), config, sources))
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	var enabled bool
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT memory_enabled FROM agent_runtime_revision WHERE revision = 'legacy'`).Scan(&enabled))
	require.False(t, enabled)
	execSQL(t, dsn, `INSERT INTO runtime_revision
		(revision, kind, namespace, source_snapshot, actor_template_atespace, actor_template_name)
		VALUES ('omitted', 'agent', 'team', '{}', 'team', 'omitted')`)
	execSQL(t, dsn, `INSERT INTO agent_revision (revision, agent_name, agent_uid, agent_card)
		VALUES ('omitted', 'assistant', 'agent-uid', $1)`, []byte{})
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT memory_enabled FROM agent_runtime_revision WHERE revision = 'omitted'`).Scan(&enabled))
	require.False(t, enabled, "new records must also fail closed when the policy is omitted")
}
