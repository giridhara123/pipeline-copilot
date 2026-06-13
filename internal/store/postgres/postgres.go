package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgvector "github.com/pgvector/pgvector-go"
	pgxvector "github.com/pgvector/pgvector-go/pgx"

	"github.com/giridhara123/pipeline-copilot/internal/store"
)

// Store implements store.Store against a real PostgreSQL + pgvector database.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool and runs the schema migration.
func New(ctx context.Context, dsn string) (*Store, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	// Register the pgvector type on every new connection in the pool.
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return pgxvector.RegisterTypes(ctx, conn)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("postgres: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		return nil, fmt.Errorf("postgres: migrate: %w", err)
	}
	return s, nil
}

func (s *Store) Close() { s.pool.Close() }

// migrate creates tables if they don't exist.
func (s *Store) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE EXTENSION IF NOT EXISTS vector;

		CREATE TABLE IF NOT EXISTS failures (
			id              BIGSERIAL PRIMARY KEY,
			repo            TEXT        NOT NULL,
			branch          TEXT        NOT NULL,
			run_id          TEXT        NOT NULL,
			run_url         TEXT        NOT NULL,
			workflow_name   TEXT        NOT NULL,
			commit_sha      TEXT        NOT NULL,
			commit_msg      TEXT        NOT NULL,
			category        TEXT        NOT NULL,
			confidence      REAL        NOT NULL,
			summary         TEXT        NOT NULL,
			root_cause      TEXT        NOT NULL,
			next_step       TEXT        NOT NULL,
			failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS failure_embeddings (
			id          BIGSERIAL PRIMARY KEY,
			failure_id  BIGINT      NOT NULL REFERENCES failures(id) ON DELETE CASCADE,
			embedding   vector(384) NOT NULL
		);

		CREATE TABLE IF NOT EXISTS flaky_tests (
			id          BIGSERIAL PRIMARY KEY,
			repo        TEXT        NOT NULL,
			test_name   TEXT        NOT NULL,
			failure_id  BIGINT      NOT NULL REFERENCES failures(id) ON DELETE CASCADE,
			failed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
	`)
	return err
}

func (s *Store) SaveFailure(ctx context.Context, f store.Failure) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO failures
			(repo, branch, run_id, run_url, workflow_name, commit_sha, commit_msg,
			 category, confidence, summary, root_cause, next_step)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		RETURNING id`,
		f.Repo, f.Branch, f.RunID, f.RunURL, f.WorkflowName,
		f.CommitSHA, f.CommitMsg,
		f.Category, f.Confidence, f.Summary, f.RootCause, f.NextStep,
	).Scan(&id)
	return id, err
}

func (s *Store) SaveEmbedding(ctx context.Context, failureID int64, embedding []float32) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO failure_embeddings (failure_id, embedding)
		VALUES ($1, $2)`,
		failureID, pgvector.NewVector(embedding),
	)
	return err
}

func (s *Store) SimilarFailures(ctx context.Context, embedding []float32, repo string, limit int) ([]store.SimilarFailure, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			f.id, f.repo, f.branch, f.run_id, f.run_url, f.workflow_name,
			f.commit_sha, f.commit_msg, f.category, f.confidence,
			f.summary, f.root_cause, f.next_step, f.failed_at,
			1 - (fe.embedding <=> $1) AS similarity
		FROM failure_embeddings fe
		JOIN failures f ON f.id = fe.failure_id
		WHERE f.repo = $2
		ORDER BY fe.embedding <=> $1
		LIMIT $3`,
		pgvector.NewVector(embedding), repo, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []store.SimilarFailure
	for rows.Next() {
		var sf store.SimilarFailure
		err := rows.Scan(
			&sf.ID, &sf.Repo, &sf.Branch, &sf.RunID, &sf.RunURL, &sf.WorkflowName,
			&sf.CommitSHA, &sf.CommitMsg, &sf.Category, &sf.Confidence,
			&sf.Summary, &sf.RootCause, &sf.NextStep, &sf.FailedAt,
			&sf.Similarity,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, sf)
	}
	return results, rows.Err()
}

func (s *Store) RecordFlakyTest(ctx context.Context, repo, testName string, failureID int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO flaky_tests (repo, test_name, failure_id)
		VALUES ($1, $2, $3)`,
		repo, testName, failureID,
	)
	return err
}

func (s *Store) FlakyTests(ctx context.Context, repo string, minCount int, windowDays int) ([]store.FlakyTest, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT test_name, COUNT(*) AS fail_count, MAX(failed_at) AS last_failed_at
		FROM flaky_tests
		WHERE repo = $1
		  AND failed_at >= NOW() - ($2 || ' days')::INTERVAL
		GROUP BY test_name
		HAVING COUNT(*) >= $3
		ORDER BY fail_count DESC`,
		repo, windowDays, minCount,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []store.FlakyTest
	for rows.Next() {
		var ft store.FlakyTest
		if err := rows.Scan(&ft.TestName, &ft.FailCount, &ft.LastFailedAt); err != nil {
			return nil, err
		}
		results = append(results, ft)
	}
	return results, rows.Err()
}
