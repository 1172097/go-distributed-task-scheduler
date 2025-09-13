-- Map any idempotency_key to a single task_id (fast lookup)
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ensure a given key on tasks is unique if present (optional but nice)
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idempotency ON tasks (idempotency_key)
WHERE
    idempotency_key IS NOT NULL;