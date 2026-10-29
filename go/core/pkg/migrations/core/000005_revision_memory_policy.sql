-- +goose Up
-- Legacy revisions have no durable evidence of an enabled memory policy.
ALTER TABLE agent_revision ADD COLUMN memory_enabled BOOLEAN NOT NULL DEFAULT FALSE;

CREATE OR REPLACE VIEW agent_runtime_revision AS
    SELECT r.*, a.agent_name, a.agent_uid, a.agent_card, a.memory_enabled
    FROM runtime_revision r JOIN agent_revision a USING (revision) WHERE r.kind = 'agent';

-- +goose Down
DROP VIEW agent_runtime_revision;
ALTER TABLE agent_revision DROP COLUMN memory_enabled;
CREATE VIEW agent_runtime_revision AS
    SELECT r.*, a.agent_name, a.agent_uid, a.agent_card
    FROM runtime_revision r JOIN agent_revision a USING (revision) WHERE r.kind = 'agent';
