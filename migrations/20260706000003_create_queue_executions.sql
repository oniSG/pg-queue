CREATE TYPE queue_executions_status AS ENUM ('running', 'succeeded', 'failed');

CREATE TABLE
    queue_executions (
        id BIGSERIAL PRIMARY KEY,
        queue_id BIGINT NOT NULL REFERENCES segment_recalculate_queue (id) ON DELETE CASCADE,
        status queue_executions_status NOT NULL DEFAULT 'running',
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW (),
        ended_at TIMESTAMPTZ,
        error JSONB
    );

CREATE INDEX queue_executions_queue_id_idx ON queue_executions (queue_id);

CREATE INDEX queue_executions_running_idx ON queue_executions (started_at)
WHERE
    status = 'running';