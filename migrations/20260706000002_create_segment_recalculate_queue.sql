CREATE TYPE queue_job_status AS ENUM ('pending', 'running', 'succeeded', 'failed');

CREATE TABLE segment_recalculate_queue (
    id BIGSERIAL PRIMARY KEY,
    segment_id UUID NOT NULL REFERENCES segments(id) ON DELETE CASCADE,
    status queue_job_status NOT NULL DEFAULT 'pending',
    scheduled_for TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX segment_recalculate_queue_ready_idx
    ON segment_recalculate_queue (scheduled_for)
    WHERE status = 'pending';

CREATE INDEX segment_recalculate_queue_segment_id_idx
    ON segment_recalculate_queue (segment_id);
