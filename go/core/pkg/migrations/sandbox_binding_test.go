package migrations

import (
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

func TestSandboxBindingMigrationUpDown(t *testing.T) {
	dsn, config := startTestDB(t)
	source := BuiltinSourcesInSchema(false, "public", "public")[0]
	require.NoError(t, WithProvider(t.Context(), config, source, func(provider *goose.Provider) error {
		_, err := provider.UpTo(t.Context(), 2)
		return err
	}))
	require.False(t, testColumnExists(t, dsn, "sandbox", "session_id"))
	require.NoError(t, RunUp(t.Context(), config, []Source{source}))
	require.True(t, testColumnExists(t, dsn, "sandbox", "session_id"))
	require.NoError(t, WithProvider(t.Context(), config, source, func(provider *goose.Provider) error {
		_, err := provider.DownTo(t.Context(), 2)
		return err
	}))
	require.False(t, testColumnExists(t, dsn, "sandbox", "session_id"))
}
