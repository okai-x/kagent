package translator_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimeconfig "github.com/kagent-dev/kagent/go/adk/pkg/config"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	configv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/config/v1"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
)

func memoryPolicyHarness() *v1alpha3.Harness {
	return &v1alpha3.Harness{
		ObjectMeta: metav1.ObjectMeta{Name: "memory-policy", Namespace: "test"},
		Spec: v1alpha3.HarnessSpec{
			Kagent:   &v1alpha3.KagentHarness{},
			Workload: v1alpha3.HarnessWorkload{Image: new("example.com/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")},
			Substrate: v1alpha3.RuntimeSubstratePolicy{
				WorkerPoolRef: corev1.LocalObjectReference{Name: "default"}, SnapshotPolicy: v1alpha3.RuntimeSnapshotPolicy{Location: "snapshots"},
			},
		},
	}
}

func TestCompiledMemoryPolicyIsPinnedAndImmutable(t *testing.T) {
	dsn := dbtest.StartT(context.WithoutCancel(t.Context()), t)
	dbtest.MigrateT(t, dsn, false)
	pool, err := database.Connect(t.Context(), &database.PostgresConfig{Pool: dbtest.PoolConfig(dsn)})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	store := database.NewClient(pool, "public")
	var sessions []*apiv1.Session
	for _, enabled := range []bool{false, true} {
		harness := memoryPolicyHarness()
		if enabled {
			harness.Spec.Kagent.Memory = &v1alpha3.KagentHarnessMemory{ModelConfigRef: v1alpha3.ObjectReference{Name: "default-model"}, TTLDays: 7}
		}
		agent := inlineAgent(harness, &v1alpha3.AgentTemplate{Spec: v1alpha3.AgentTemplateSpec{ModelConfig: &v1alpha3.ObjectReference{Name: "default-model"}, SystemPrompt: "help"}})
		agent.UID = "agent-uid"
		compiled, err := compiler(t, modelConfig(), harness).CompileAgent(t.Context(), agent)
		require.NoError(t, err)
		var config configv1.ADKConfig
		require.NoError(t, runtimeconfig.Unmarshal(compiled.ConfigJSON, &config))
		require.Equal(t, enabled, config.Agent.Memory != nil)
		require.Equal(t, enabled, compiled.MemoryEnabled)
		digest, err := compiled.Digest()
		require.NoError(t, err)
		revision := database.RuntimeRevision{
			Revision: digest.String(), Namespace: agent.Namespace, AgentName: agent.Name, AgentUID: string(agent.UID),
			SourceSnapshot: compiled.Provenance, AgentCard: compiled.AgentCard, MemoryEnabled: compiled.MemoryEnabled,
			ActorTemplateAtespace: "test", ActorTemplateName: "template-" + digest.Short(), ActorTemplateUID: uuid.NewString(),
		}
		require.NoError(t, store.UpsertAgentDefinition(t.Context(), database.AgentDefinition{
			Namespace: agent.Namespace, AgentName: agent.Name, AgentUID: string(agent.UID), DesiredRevision: revision.Revision,
		}))
		require.NoError(t, store.RecordRuntimeRevision(t.Context(), revision, database.RecordRevisionOptions{Ready: true}))
		session, _, err := store.CreateSession(t.Context(), &apiv1.Session{
			Id: uuid.NewString(), Creator: "shared-product-owner", Agent: &apiv1.ResourceReference{Namespace: agent.Namespace, Name: agent.Name},
		}, uuid.NewString())
		require.NoError(t, err)
		sessions = append(sessions, session)
		// Re-recording a hash must never change existing Session authority, in either direction.
		revision.MemoryEnabled = !enabled
		require.NoError(t, store.RecordRuntimeRevision(t.Context(), revision, database.RecordRevisionOptions{Ready: true}))
	}
	for i, session := range sessions {
		pinned, err := store.GetRuntimeRevision(t.Context(), session.PreparedRevision)
		require.NoError(t, err)
		require.Equal(t, i == 1, pinned.MemoryEnabled)
	}
	require.NotEqual(t, sessions[0].PreparedRevision, sessions[1].PreparedRevision, "a genuine Memory configuration change produces a new revision")
}

func TestCompileMemoryPolicyRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, model string
		ttl         int
	}{
		{name: "missing embedding model", model: "absent", ttl: 7},
		{name: "invalid TTL", model: "default-model", ttl: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			harness := memoryPolicyHarness()
			harness.Spec.Kagent.Memory = &v1alpha3.KagentHarnessMemory{ModelConfigRef: v1alpha3.ObjectReference{Name: tc.model}, TTLDays: tc.ttl}
			agent := inlineAgent(harness, &v1alpha3.AgentTemplate{Spec: v1alpha3.AgentTemplateSpec{ModelConfig: &v1alpha3.ObjectReference{Name: "default-model"}, SystemPrompt: "help"}})
			compiled, err := compiler(t, modelConfig(), harness).CompileAgent(t.Context(), agent)
			require.Error(t, err)
			require.Nil(t, compiled, "invalid configuration cannot authorize Memory")
		})
	}
}
