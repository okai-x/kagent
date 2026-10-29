-- +goose Up
CREATE TABLE session_workspace_access (
    session_id UUID PRIMARY KEY REFERENCES session(id) ON DELETE CASCADE,
    grant_id UUID NOT NULL UNIQUE,
    executor_id UUID NOT NULL,
    head_task_id TEXT NOT NULL CHECK (head_task_id <> ''),
    head_version BIGINT NOT NULL CHECK (head_version > 0),
    actor_uid TEXT NOT NULL CHECK (actor_uid <> ''),
    path TEXT NOT NULL CHECK (path <> ''),
    phase TEXT NOT NULL CHECK (phase IN ('PREPARING', 'READING', 'CLEANUP', 'QUIESCING', 'RELEASED')),
    consumed BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at TIMESTAMPTZ NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    snapshot_generation INTEGER CHECK (snapshot_generation >= 0),
    CHECK (phase <> 'QUIESCING' OR snapshot_generation IS NOT NULL),
    CHECK (NOT consumed OR phase <> 'PREPARING')
);

-- +goose Down
DROP TABLE session_workspace_access;
