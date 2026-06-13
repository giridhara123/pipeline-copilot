package fake

import (
	"context"
	"fmt"
	"time"

	"github.com/giridhara123/pipeline-copilot/internal/events"
	"github.com/giridhara123/pipeline-copilot/internal/provider"
)

// Provider is a deterministic, in-memory provider for tests and local dev.
// It never makes network calls.
type Provider struct {
	// Inject these to control what the fake returns.
	LogContent   string
	DiffContent  string
	ShouldError  bool
}

func New() *Provider {
	return &Provider{
		LogContent: `
##[group]Run tests
##[error]FAIL: TestUserLogin (0.23s)
    auth_test.go:42: expected status 200, got 401
##[endgroup]
##[error]Process completed with exit code 1.
`,
		DiffContent: `diff --git a/auth/login.go b/auth/login.go
+++ b/auth/login.go
-  return token, nil
+  return nil, errors.New("not implemented")
`,
	}
}

func (f *Provider) Name() string { return "fake" }

func (f *Provider) VerifyWebhook(headers map[string]string, body []byte) error {
	if f.ShouldError {
		return fmt.Errorf("fake: webhook verification failed")
	}
	return nil
}

func (f *Provider) ParseEvent(headers map[string]string, body []byte) (events.CanonicalEvent, error) {
	return events.CanonicalEvent{
		Type:      events.EventPipelineFailed,
		Provider:  "fake",
		Repo:      "giridhara123/pipeline-copilot",
		Timestamp: time.Now(),
		Payload: events.PipelineFailedPayload{
			RunID:        "fake-run-001",
			RunURL:       "https://github.com/giridhara123/pipeline-copilot/actions/runs/1",
			Branch:       "main",
			CommitSHA:    "abc1234",
			CommitMsg:    "feat: add login endpoint",
			WorkflowName: "CI",
			FailedStep:   "Run tests",
		},
	}, nil
}

func (f *Provider) FetchLogs(ctx context.Context, runID string) (provider.RawLog, error) {
	if f.ShouldError {
		return provider.RawLog{}, fmt.Errorf("fake: failed to fetch logs for run %s", runID)
	}
	return provider.RawLog{
		Content: f.LogContent,
		RunID:   runID,
		RunURL:  "https://github.com/giridhara123/pipeline-copilot/actions/runs/1",
	}, nil
}

func (f *Provider) FetchDiff(ctx context.Context, prNumber int, repo string) (provider.Diff, error) {
	if f.ShouldError {
		return provider.Diff{}, fmt.Errorf("fake: failed to fetch diff for PR #%d", prNumber)
	}
	return provider.Diff{
		Content:      f.DiffContent,
		FilesChanged: 1,
		Additions:    1,
		Deletions:    1,
	}, nil
}

func (f *Provider) ListRecentCommits(ctx context.Context, repo, branch string, limit int) ([]provider.Commit, error) {
	return []provider.Commit{
		{SHA: "abc1234", Message: "feat: add login endpoint", Author: "dev@example.com"},
		{SHA: "def5678", Message: "fix: correct token expiry", Author: "dev@example.com"},
	}, nil
}

func (f *Provider) GetDeploymentStatus(ctx context.Context, deployID, repo string) (provider.DeployStatus, error) {
	return provider.DeployStatus{
		ID:          deployID,
		Environment: "production",
		State:       "failure",
		URL:         "https://example.com",
	}, nil
}

func (f *Provider) RerunJob(ctx context.Context, runID, repo string) error {
	if f.ShouldError {
		return fmt.Errorf("fake: rerun failed")
	}
	return nil
}

func (f *Provider) MergePR(ctx context.Context, prNumber int, repo string, strategy provider.MergeStrategy) error {
	if f.ShouldError {
		return fmt.Errorf("fake: merge failed")
	}
	return nil
}

func (f *Provider) ReviewPR(ctx context.Context, prNumber int, repo, decision, comment string) error {
	return nil
}

func (f *Provider) ApproveDeployment(ctx context.Context, deployID, repo, decision string) error {
	return nil
}

func (f *Provider) Rollback(ctx context.Context, deployID, repo string) error {
	if f.ShouldError {
		return fmt.Errorf("fake: rollback failed")
	}
	return nil
}
