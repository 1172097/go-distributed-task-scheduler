CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TYPE task_status AS ENUM ('queued','running','succeeded','failed');
CREATE TYPE task_priority AS ENUM ('critical','high','default','low');

CREATE TABLE tasks (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  type TEXT NOT NULL,
  priority task_priority NOT NULL DEFAULT 'default',
  payload JSONB NOT NULL,
  idempotency_key TEXT,
  status task_status NOT NULL DEFAULT 'queued',
  max_retries INT NOT NULL DEFAULT 5,
  attempts INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idem ON tasks(idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE task_runs (
  id BIGSERIAL PRIMARY KEY,
  task_id UUID REFERENCES tasks(id) ON DELETE CASCADE,
  attempt INT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('running','succeeded','failed')),
  error TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ
);
