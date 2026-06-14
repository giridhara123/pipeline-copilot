package diagnosis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type Request struct {
	RunID        string `json:"run_id"`
	Repo         string `json:"repo"`
	Branch       string `json:"branch"`
	CommitSHA    string `json:"commit_sha"`
	CommitMsg    string `json:"commit_msg"`
	WorkflowName string `json:"workflow_name"`
	LogContent   string `json:"log_content"`
}

type Result struct {
	Summary    string  `json:"summary"`
	RootCause  string  `json:"root_cause"`
	Category   string  `json:"category"`
	Confidence float64 `json:"confidence"`
	NextStep   string  `json:"next_step"`
}

type PRSummaryRequest struct {
	PRNumber    int    `json:"pr_number"`
	Title       string `json:"title"`
	Author      string `json:"author"`
	Repo        string `json:"repo"`
	BaseBranch  string `json:"base_branch"`
	DiffContent string `json:"diff_content"`
}

type PRSummaryResult struct {
	Summary   string   `json:"summary"`
	RiskLevel string   `json:"risk_level"` // low | medium | high
	RiskFlags []string `json:"risk_flags"`
	Checklist []string `json:"checklist"`
}

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{},
	}
}

func (c *Client) Diagnose(ctx context.Context, req Request) (Result, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return Result{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/diagnose", bytes.NewReader(body))
	if err != nil {
		return Result{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return Result{}, fmt.Errorf("diagnosis: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Result{}, fmt.Errorf("diagnosis: service returned %d", resp.StatusCode)
	}

	var result Result
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return Result{}, fmt.Errorf("diagnosis: failed to decode response: %w", err)
	}
	return result, nil
}

// Embed generates a vector embedding for the given text by calling the AI service.
// The returned slice has 1536 dimensions, suitable for pgvector storage.
func (c *Client) Embed(ctx context.Context, text string) ([]float32, error) {
	body, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/embed", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("embed: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embed: service returned %d", resp.StatusCode)
	}

	var result struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("embed: failed to decode response: %w", err)
	}
	return result.Embedding, nil
}

// SummarizePR calls the AI service to summarise a pull request diff.
func (c *Client) SummarizePR(ctx context.Context, req PRSummaryRequest) (PRSummaryResult, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return PRSummaryResult{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/summarize-pr", bytes.NewReader(body))
	if err != nil {
		return PRSummaryResult{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return PRSummaryResult{}, fmt.Errorf("summarize-pr: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return PRSummaryResult{}, fmt.Errorf("summarize-pr: service returned %d", resp.StatusCode)
	}

	var result PRSummaryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return PRSummaryResult{}, fmt.Errorf("summarize-pr: failed to decode response: %w", err)
	}
	return result, nil
}
// PipelineCopilot Phase 3 — real-time PR feed with AI risk analysis
