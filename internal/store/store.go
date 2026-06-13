package store

import (
	"context"
	"time"
)

// Failure is one recorded pipeline failure with its AI diagnosis.
type Failure struct {
	ID           int64
	Repo         string
	Branch       string
	RunID        string
	RunURL       string
	WorkflowName string
	CommitSHA    string
	CommitMsg    string
	Category     string
	Confidence   float64
	Summary      string
	RootCause    string
	NextStep     string
	FailedAt     time.Time
}

// SimilarFailure is a past failure returned by semantic search, with its similarity score.
type SimilarFailure struct {
	Failure
	Similarity float64 // cosine similarity 0-1; higher = more similar
}

// FlakyTest is a test name that has failed multiple times across different commits.
type FlakyTest struct {
	TestName    string
	FailCount   int
	LastFailedAt time.Time
}

// Store is the persistence contract. The gateway only depends on this interface.
type Store interface {
	// SaveFailure inserts a failure record and returns its new ID.
	SaveFailure(ctx context.Context, f Failure) (int64, error)

	// SaveEmbedding stores the vector embedding for a failure (for RAG).
	SaveEmbedding(ctx context.Context, failureID int64, embedding []float32) error

	// SimilarFailures returns up to `limit` past failures semantically closest
	// to the given embedding. Used to inject historical context into the AI prompt.
	SimilarFailures(ctx context.Context, embedding []float32, repo string, limit int) ([]SimilarFailure, error)

	// RecordFlakyTest logs that a named test failed in a given failure record.
	RecordFlakyTest(ctx context.Context, repo, testName string, failureID int64) error

	// FlakyTests returns tests in a repo that have failed >= minCount times
	// within the last windowDays days.
	FlakyTests(ctx context.Context, repo string, minCount int, windowDays int) ([]FlakyTest, error)

	// Close releases the connection pool.
	Close()
}
