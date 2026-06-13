package github

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/giridhara123/pipeline-copilot/internal/events"
	"github.com/giridhara123/pipeline-copilot/internal/provider"
)

// Provider implements provider.Provider for GitHub Actions + GitHub PRs.
type Provider struct {
	webhookSecret string
	token         string
}

func New(webhookSecret, token string) *Provider {
	return &Provider{
		webhookSecret: webhookSecret,
		token:         token,
	}
}

func (p *Provider) Name() string { return "github" }

// VerifyWebhook validates the X-Hub-Signature-256 header.
func (p *Provider) VerifyWebhook(headers map[string]string, body []byte) error {
	sig, ok := headers["x-hub-signature-256"]
	if !ok {
		return fmt.Errorf("github: missing X-Hub-Signature-256 header")
	}
	mac := hmac.New(sha256.New, []byte(p.webhookSecret))
	mac.Write(body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(sig), []byte(expected)) {
		return fmt.Errorf("github: webhook signature mismatch")
	}
	return nil
}

// ParseEvent translates a raw GitHub webhook payload into a canonical event.
func (p *Provider) ParseEvent(headers map[string]string, body []byte) (events.CanonicalEvent, error) {
	eventType := headers["x-github-event"]

	switch eventType {
	case "workflow_run":
		return p.parseWorkflowRun(body)
	case "pull_request":
		return p.parsePullRequest(body)
	default:
		return events.CanonicalEvent{}, fmt.Errorf("github: unsupported event type: %s", eventType)
	}
}

func (p *Provider) parseWorkflowRun(body []byte) (events.CanonicalEvent, error) {
	var payload struct {
		Action      string `json:"action"`
		WorkflowRun struct {
			ID         int64  `json:"id"`
			Name       string `json:"name"`
			Conclusion string `json:"conclusion"`
			HTMLURL    string `json:"html_url"`
			HeadBranch string `json:"head_branch"`
			HeadSHA    string `json:"head_sha"`
			HeadCommit struct {
				Message string `json:"message"`
			} `json:"head_commit"`
		} `json:"workflow_run"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		return events.CanonicalEvent{}, fmt.Errorf("github: failed to parse workflow_run: %w", err)
	}

	if payload.Action != "completed" || payload.WorkflowRun.Conclusion != "failure" {
		return events.CanonicalEvent{}, fmt.Errorf("github: ignoring non-failure workflow_run")
	}

	return events.CanonicalEvent{
		Type:      events.EventPipelineFailed,
		Provider:  "github",
		Repo:      payload.Repository.FullName,
		Timestamp: time.Now(),
		Payload: events.PipelineFailedPayload{
			RunID:        fmt.Sprintf("%d", payload.WorkflowRun.ID),
			RunURL:       payload.WorkflowRun.HTMLURL,
			Branch:       payload.WorkflowRun.HeadBranch,
			CommitSHA:    payload.WorkflowRun.HeadSHA,
			CommitMsg:    strings.Split(payload.WorkflowRun.HeadCommit.Message, "\n")[0],
			WorkflowName: payload.WorkflowRun.Name,
		},
	}, nil
}

func (p *Provider) parsePullRequest(body []byte) (events.CanonicalEvent, error) {
	var payload struct {
		Action      string `json:"action"`
		PullRequest struct {
			Number  int    `json:"number"`
			Title   string `json:"title"`
			HTMLURL string `json:"html_url"`
			Head    struct {
				SHA string `json:"sha"`
			} `json:"head"`
			Base struct {
				Ref string `json:"ref"`
			} `json:"base"`
			User struct {
				Login string `json:"login"`
			} `json:"user"`
		} `json:"pull_request"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		return events.CanonicalEvent{}, fmt.Errorf("github: failed to parse pull_request: %w", err)
	}

	var eventType events.EventType
	switch payload.Action {
	case "opened", "reopened":
		eventType = events.EventPROpened
	case "closed":
		eventType = events.EventPRClosed
	default:
		return events.CanonicalEvent{}, fmt.Errorf("github: ignoring PR action: %s", payload.Action)
	}

	return events.CanonicalEvent{
		Type:      eventType,
		Provider:  "github",
		Repo:      payload.Repository.FullName,
		Timestamp: time.Now(),
		Payload: events.PRPayload{
			Number:     payload.PullRequest.Number,
			Title:      payload.PullRequest.Title,
			Author:     payload.PullRequest.User.Login,
			URL:        payload.PullRequest.HTMLURL,
			HeadSHA:    payload.PullRequest.Head.SHA,
			BaseBranch: payload.PullRequest.Base.Ref,
		},
	}, nil
}

// FetchLogs downloads the zip of logs for a workflow run and returns the text content.
// GitHub returns logs as a zip archive; we extract and concatenate all log files.
func (p *Provider) FetchLogs(ctx context.Context, runID string) (provider.RawLog, error) {
	// Step 1: get the redirect URL for the log zip.
	parts := strings.SplitN(runID, "/", 3) // "owner/repo/runid"
	if len(parts) != 3 {
		return provider.RawLog{}, fmt.Errorf("github: runID must be owner/repo/runid, got %q", runID)
	}
	owner, repo, id := parts[0], parts[1], parts[2]

	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/actions/runs/%s/logs", owner, repo, id)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return provider.RawLog{}, err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Accept", "application/vnd.github+json")

	// Use a client that follows redirects.
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return provider.RawLog{}, fmt.Errorf("github: log request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return provider.RawLog{}, fmt.Errorf("github: log fetch returned %d", resp.StatusCode)
	}

	zipData, err := io.ReadAll(io.LimitReader(resp.Body, 50<<20)) // 50 MB cap
	if err != nil {
		return provider.RawLog{}, fmt.Errorf("github: reading log zip: %w", err)
	}

	// Step 2: unzip and concatenate log files.
	zr, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return provider.RawLog{}, fmt.Errorf("github: unzipping logs: %w", err)
	}

	var sb strings.Builder
	for _, f := range zr.File {
		if !strings.HasSuffix(f.Name, ".txt") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			continue
		}
		content, _ := io.ReadAll(io.LimitReader(rc, 2<<20)) // 2 MB per file
		rc.Close()
		sb.WriteString(fmt.Sprintf("\n=== %s ===\n", f.Name))
		sb.Write(content)
	}

	runURL := fmt.Sprintf("https://github.com/%s/%s/actions/runs/%s", owner, repo, id)
	return provider.RawLog{
		Content: sb.String(),
		RunID:   runID,
		RunURL:  runURL,
	}, nil
}

func (p *Provider) FetchDiff(ctx context.Context, prNumber int, repo string) (provider.Diff, error) {
	return provider.Diff{}, fmt.Errorf("github: FetchDiff not yet implemented — coming in Phase 3")
}

func (p *Provider) ListRecentCommits(ctx context.Context, repo, branch string, limit int) ([]provider.Commit, error) {
	return nil, fmt.Errorf("github: ListRecentCommits not yet implemented — coming in Phase 4")
}

func (p *Provider) GetDeploymentStatus(ctx context.Context, deployID, repo string) (provider.DeployStatus, error) {
	return provider.DeployStatus{}, fmt.Errorf("github: GetDeploymentStatus not yet implemented — coming in Phase 4")
}

// Write operations — Phase 5 will fill these in.

func (p *Provider) RerunJob(ctx context.Context, runID, repo string) error {
	return fmt.Errorf("github: RerunJob not yet implemented — coming in Phase 5")
}

func (p *Provider) MergePR(ctx context.Context, prNumber int, repo string, strategy provider.MergeStrategy) error {
	return fmt.Errorf("github: MergePR not yet implemented — coming in Phase 5")
}

func (p *Provider) ReviewPR(ctx context.Context, prNumber int, repo, decision, comment string) error {
	return fmt.Errorf("github: ReviewPR not yet implemented — coming in Phase 5")
}

func (p *Provider) ApproveDeployment(ctx context.Context, deployID, repo, decision string) error {
	return fmt.Errorf("github: ApproveDeployment not yet implemented — coming in Phase 5")
}

func (p *Provider) Rollback(ctx context.Context, deployID, repo string) error {
	return fmt.Errorf("github: Rollback not yet implemented — coming in Phase 5")
}
