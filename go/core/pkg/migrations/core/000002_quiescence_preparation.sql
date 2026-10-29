-- +goose Up
ALTER TABLE session_task_event
    ADD COLUMN quiescence_phase TEXT,
    ADD COLUMN quiescence_expires_at TIMESTAMPTZ,
    ADD COLUMN quiescence_actor_uid TEXT,
    ADD COLUMN quiescence_snapshot_generation INTEGER;

-- Existing claims may already have issued runtime work. They must never
-- become retryable merely because this migration cannot recover their inputs.
UPDATE session_task_event SET quiescence_phase = 'ISSUED'
    WHERE quiescence_executor_id IS NOT NULL;

ALTER TABLE session_task_event
    ADD CONSTRAINT session_quiescence_phase CHECK (
        (quiescence_executor_id IS NULL AND quiescence_phase IS NULL)
        OR (quiescence_executor_id IS NOT NULL AND quiescence_phase IS NOT NULL
            AND quiescence_phase IN ('PREPARING', 'ISSUED'))),
    ADD CONSTRAINT session_quiescence_preparation_expiry CHECK (
        quiescence_phase IS NULL
        OR (quiescence_phase = 'PREPARING' AND quiescence_expires_at IS NOT NULL)
        OR (quiescence_phase = 'ISSUED' AND quiescence_expires_at IS NULL)),
    ADD CONSTRAINT session_quiescence_actor CHECK (
        (quiescence_actor_uid IS NULL AND quiescence_snapshot_generation IS NULL)
        OR (quiescence_phase IS NOT NULL AND quiescence_phase = 'ISSUED'
            AND quiescence_actor_uid IS NOT NULL AND quiescence_actor_uid <> ''
            AND quiescence_snapshot_generation IS NOT NULL AND quiescence_snapshot_generation >= 0));

-- +goose Down
ALTER TABLE session_task_event
    DROP CONSTRAINT session_quiescence_actor,
    DROP CONSTRAINT session_quiescence_preparation_expiry,
    DROP CONSTRAINT session_quiescence_phase,
    DROP COLUMN quiescence_snapshot_generation,
    DROP COLUMN quiescence_actor_uid,
    DROP COLUMN quiescence_expires_at,
    DROP COLUMN quiescence_phase;
