package provider

import (
	"context"

	"github.com/giridhara123/pipeline-copilot/internal/events"
)

// RawLog is the raw text fetched from a CI run.
type RawLog struct {
	Content  string
	RunID    string
	RunURL   string
	TruncatedBytes int
}

// Diff is the raw diff text for a pull request.
type Diff struct {
	Content    string
	FilesChanged int
	Additions  int
	Deletions  int
}

// Commit is a single commit summary.
type Commit struct {
	SHA     string
	Message string
	Author  string
}

// DeployStatus is the current state of a deployment.
type DeployStatus struct {
	ID          string
	Environment string
	State       string // pending | success | failure | in_progress
	URL         string
}

// MergeStrategy controls how a PR is merged.
type MergeStrategy string

const (
	MergeStrategyMerge  MergeStrategy = "merge"
	MergeStrategySquash MergeStrategy = "squash"
	MergeStrategyRebase MergeStrategy = "rebase"
)

// Provider is the plugin contract every SCM/CI adapter must implement.
// The core engine only depends on this interface — never on GitHub types directly.
type Provider interface {
	Name() string

	// Inbound: verify and parse a raw webhook into a canonical event.
	VerifyWebhook(headers map[string]string, body []byte) error
	ParseEvent(headers map[string]string, body []byte) (events.CanonicalEvent, error)

	// Read operations — used by the read-only agent tools.
	FetchLogs(ctx context.Context, runID string) (RawLog, error)
	FetchDiff(ctx context.Context, prNumber int, repo string) (Diff, error)
	ListRecentCommits(ctx context.Context, repo string, branch string, limit int) ([]Commit, error)
	GetDeploymentStatus(ctx context.Context, deployID string, repo string) (DeployStatus, error)

	// Write operations — always gated by RBAC + human confirmation in the core.
	RerunJob(ctx context.Context, runID string, repo string) error
	MergePR(ctx context.Context, prNumber int, repo string, strategy MergeStrategy) error
	ReviewPR(ctx context.Context, prNumber int, repo string, decision string, comment string) error
	ApproveDeployment(ctx context.Context, deployID string, repo string, decision string) error
	Rollback(ctx context.Context, deployID string, repo string) error
}
