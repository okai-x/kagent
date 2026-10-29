-- +goose Up
ALTER TABLE sandbox ADD COLUMN session_id UUID REFERENCES session(id) ON DELETE RESTRICT;
CREATE INDEX sandbox_session_idx ON sandbox (session_id, id) WHERE session_id IS NOT NULL;
CREATE OR REPLACE VIEW sandbox_record AS
    SELECT r.id, r.user_id, r.request_id, r.prepared_revision, r.state, r.operation, r.operation_id, r.executor_id,
        s.data, s.namespace, s.sandbox_template_name, s.revision_receipt, s.request_hash, s.expires_at, s.session_id
    FROM runtime_instance r JOIN sandbox s USING (id) WHERE r.kind = 'sandbox';

-- +goose Down
DROP VIEW sandbox_record;
CREATE VIEW sandbox_record AS
    SELECT r.id, r.user_id, r.request_id, r.prepared_revision, r.state, r.operation, r.operation_id, r.executor_id,
        s.data, s.namespace, s.sandbox_template_name, s.revision_receipt, s.request_hash, s.expires_at
    FROM runtime_instance r JOIN sandbox s USING (id) WHERE r.kind = 'sandbox';
ALTER TABLE sandbox DROP COLUMN session_id;
