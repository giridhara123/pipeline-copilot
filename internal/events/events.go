package events

import "time"

type EventType string

const (
	EventPipelineFailed    EventType = "pipeline_failed"
	EventPipelineSucceeded EventType = "pipeline_succeeded"
	EventPROpened          EventType = "pr_opened"
	EventPRReviewed        EventType = "pr_reviewed"
	EventPRClosed          EventType = "pr_closed"
	EventChecksCompleted   EventType = "checks_completed"
	EventDeploymentPending EventType = "deployment_pending"
	EventDeploymentFailed  EventType = "deployment_failed"
)

type CanonicalEvent struct {
	Type      EventType
	Provider  string
	Repo      string
	Timestamp time.Time
	Payload   any
}

// PipelineFailedPayload is attached when Type == EventPipelineFailed.
type PipelineFailedPayload struct {
	RunID      string
	RunURL     string
	Branch     string
	CommitSHA  string
	CommitMsg  string
	WorkflowName string
	FailedStep string
}

// PRPayload is attached for PR open/review/close events.
type PRPayload struct {
	Number    int
	Title     string
	Author    string
	URL       string
	HeadSHA   string
	BaseBranch string
	Decision  string // approve | request_changes | closed | merged
}

// DeploymentPayload is attached for deployment events.
type DeploymentPayload struct {
	DeployID    string
	Environment string
	URL         string
}
// v2 PR card with GitHub preview
