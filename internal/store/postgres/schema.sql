CREATE EXTENSION IF NOT EXISTS vector;

-- Every pipeline failure event and its AI diagnosis.
CREATE TABLE IF NOT EXISTS failures (
    id              BIGSERIAL PRIMARY KEY,
    repo            TEXT        NOT NULL,
    branch          TEXT        NOT NULL,
    run_id          TEXT        NOT NULL,
    run_url         TEXT        NOT NULL,
    workflow_name   TEXT        NOT NULL,
    commit_sha      TEXT        NOT NULL,
    commit_msg      TEXT        NOT NULL,
    category        TEXT        NOT NULL,  -- from AI: test_failure, code_defect, etc.
    confidence      REAL        NOT NULL,
    summary         TEXT        NOT NULL,
    root_cause      TEXT        NOT NULL,
    next_step       TEXT        NOT NULL,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS failures_repo_idx    ON failures (repo);
CREATE INDEX IF NOT EXISTS failures_branch_idx  ON failures (branch, failed_at DESC);
CREATE INDEX IF NOT EXISTS failures_category_idx ON failures (category);

-- Vector embeddings for semantic similarity search (RAG).
-- Each row pairs a failure with its 1536-dim embedding of (summary + root_cause).
CREATE TABLE IF NOT EXISTS failure_embeddings (
    id          BIGSERIAL PRIMARY KEY,
    failure_id  BIGINT      NOT NULL REFERENCES failures(id) ON DELETE CASCADE,
    embedding   vector(384) NOT NULL
);

CREATE INDEX IF NOT EXISTS failure_embeddings_ivfflat_idx
    ON failure_embeddings USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 10);

-- Flaky test tracker: counts per (repo, test_name) over a rolling window.
CREATE TABLE IF NOT EXISTS flaky_tests (
    id          BIGSERIAL PRIMARY KEY,
    repo        TEXT        NOT NULL,
    test_name   TEXT        NOT NULL,
    failure_id  BIGINT      NOT NULL REFERENCES failures(id) ON DELETE CASCADE,
    failed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS flaky_tests_repo_test_idx ON flaky_tests (repo, test_name, failed_at DESC);
