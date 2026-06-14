package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"

	"github.com/giridhara123/pipeline-copilot/internal/diagnosis"
	"github.com/giridhara123/pipeline-copilot/internal/events"
	fakeprovider "github.com/giridhara123/pipeline-copilot/internal/provider/fake"
	githubprovider "github.com/giridhara123/pipeline-copilot/internal/provider/github"
	"github.com/giridhara123/pipeline-copilot/internal/provider"
	"github.com/giridhara123/pipeline-copilot/internal/store"
	pgstore "github.com/giridhara123/pipeline-copilot/internal/store/postgres"
	"github.com/giridhara123/pipeline-copilot/internal/webhook"
)

func main() {
	_ = godotenv.Load()
	cfg := loadConfig()

	var p provider.Provider
	switch cfg.provider {
	case "github":
		p = githubprovider.New(cfg.githubWebhookSecret, cfg.githubToken)
		log.Println("provider: github")
	default:
		p = fakeprovider.New()
		log.Println("provider: fake (set PROVIDER=github for real GitHub events)")
	}

	ai := diagnosis.NewClient(cfg.aiServiceURL)

	// Connect to PostgreSQL.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := pgstore.New(ctx, cfg.databaseURL)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer db.Close()
	log.Println("store: connected to PostgreSQL")

	api := slack.New(
		cfg.slackBotToken,
		slack.OptionAppLevelToken(cfg.slackAppToken),
	)
	client := socketmode.New(api)

	ready := make(chan struct{})

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("shutting down...")
		cancel()
	}()

	eventHandler := func(ctx context.Context, evt events.CanonicalEvent) {
		switch evt.Type {
		case events.EventPipelineFailed:
			handlePipelineFailed(ctx, evt, p, ai, db, api, cfg)
		case events.EventPROpened:
			handlePROpened(ctx, evt, p, ai, api, cfg)
		default:
			log.Printf("unhandled event type: %s", evt.Type)
		}
	}

	webhookServer := webhook.NewServer(p, eventHandler)
	go func() {
		log.Printf("webhook server listening on :%s", cfg.webhookPort)
		if err := http.ListenAndServe(":"+cfg.webhookPort, webhookServer); err != nil {
			log.Fatalf("webhook server error: %v", err)
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evt := <-client.Events:
				handleSocketEvent(ctx, evt, client, api, p, cfg, ready)
			}
		}
	}()

	log.Println("PipelineCopilot gateway starting...")

	if cfg.provider != "github" {
		go func() {
			select {
			case <-ready:
			case <-ctx.Done():
				return
			}
			evt, err := p.ParseEvent(nil, nil)
			if err != nil {
				log.Printf("fake event error: %v", err)
				return
			}
			log.Printf("firing synthetic event: %s", evt.Type)
			eventHandler(ctx, evt)
		}()
	}

	if err := client.RunContext(ctx); err != nil {
		log.Fatalf("socket mode error: %v", err)
	}
}

func handlePipelineFailed(ctx context.Context, evt events.CanonicalEvent, p provider.Provider, ai *diagnosis.Client, db store.Store, api *slack.Client, cfg config) {
	payload, ok := evt.Payload.(events.PipelineFailedPayload)
	if !ok {
		return
	}

	log.Printf("diagnosing failure: run=%s repo=%s", payload.RunID, evt.Repo)

	placeholderText := fmt.Sprintf(":hourglass: *Investigating pipeline failure* in `%s` — `%s` on `%s`...", evt.Repo, payload.WorkflowName, payload.Branch)
	channelID, ts, err := api.PostMessageContext(ctx, cfg.slackChannel, slack.MsgOptionText(placeholderText, false))
	if err != nil {
		log.Printf("slack placeholder post error: %v", err)
	}

	runRef := evt.Repo + "/" + payload.RunID
	rawLog, err := p.FetchLogs(ctx, runRef)
	if err != nil {
		log.Printf("fetch logs error: %v", err)
		updateSlackMessage(ctx, api, channelID, ts,
			fmt.Sprintf(":red_circle: *Pipeline Failed* — `%s`\nCould not fetch logs: %v\n<%s|View Run>", payload.WorkflowName, err, payload.RunURL))
		return
	}

	// Step 1: embed the timestamp-stripped log to query for similar past failures.
	// The sentence-transformers model understands semantic meaning, so similar
	// error messages get similar vectors even if wording differs slightly.
	logText := stripTimestamps(rawLog.Content)
	if len(logText) > 4000 {
		logText = logText[len(logText)-4000:]
	}
	logEmbedding, logEmbedErr := ai.Embed(ctx, logText)

	// Step 2: fetch similar past failures and build RAG context for Claude.
	var similarCtx string
	if logEmbedErr == nil && len(logEmbedding) > 0 {
		similar, err := db.SimilarFailures(ctx, logEmbedding, evt.Repo, 3)
		if err != nil {
			log.Printf("rag: lookup error: %v", err)
		} else {
			log.Printf("rag: found %d similar past failures", len(similar))
			var sb strings.Builder
			injected := 0
			for i, s := range similar {
				log.Printf("rag:   [%d] id=%d similarity=%.3f category=%s age=%s", i+1, s.ID, s.Similarity, s.Category, formatAge(s.FailedAt))
				if s.Similarity < 0.7 {
					log.Printf("rag:   [%d] skipped (similarity below 0.7 threshold)", i+1)
					continue
				}
				if injected == 0 {
					sb.WriteString("\n\nPast similar failures in this repo (for context):\n")
				}
				sb.WriteString(fmt.Sprintf("%d. [%s] category=%s confidence=%.0f%%\n   Summary: %s\n   Root cause: %s\n   Fix that worked: %s\n\n",
					i+1, formatAge(s.FailedAt), s.Category, s.Confidence*100, s.Summary, s.RootCause, s.NextStep))
				injected++
			}
			if injected > 0 {
				similarCtx = sb.String()
				log.Printf("rag: injected %d past failures into Claude prompt", injected)
			} else {
				log.Printf("rag: no past failures met the similarity threshold — diagnosing cold")
			}
		}
	} else if logEmbedErr != nil {
		log.Printf("rag: embed error: %v", logEmbedErr)
	}

	// Step 3: diagnose with RAG context appended to the log.
	result, err := ai.Diagnose(ctx, diagnosis.Request{
		RunID:        payload.RunID,
		Repo:         evt.Repo,
		Branch:       payload.Branch,
		CommitSHA:    payload.CommitSHA,
		CommitMsg:    payload.CommitMsg,
		WorkflowName: payload.WorkflowName,
		LogContent:   rawLog.Content + similarCtx,
	})
	if err != nil {
		log.Printf("diagnosis error: %v", err)
		updateSlackMessage(ctx, api, channelID, ts,
			fmt.Sprintf(":red_circle: *Pipeline Failed* — `%s`\nDiagnosis unavailable.\n<%s|View Run>", payload.WorkflowName, payload.RunURL))
		return
	}

	// Step 4: persist failure and save the log embedding for future lookups.
	failureID, saveErr := db.SaveFailure(ctx, store.Failure{
		Repo:         evt.Repo,
		Branch:       payload.Branch,
		RunID:        payload.RunID,
		RunURL:       payload.RunURL,
		WorkflowName: payload.WorkflowName,
		CommitSHA:    payload.CommitSHA,
		CommitMsg:    payload.CommitMsg,
		Category:     result.Category,
		Confidence:   result.Confidence,
		Summary:      result.Summary,
		RootCause:    result.RootCause,
		NextStep:     result.NextStep,
	})
	if saveErr != nil {
		log.Printf("store: save failure error: %v", saveErr)
	} else {
		log.Printf("store: saved failure id=%d", failureID)

		if logEmbedErr == nil && len(logEmbedding) > 0 {
			if err := db.SaveEmbedding(ctx, failureID, logEmbedding); err != nil {
				log.Printf("store: save embedding error: %v", err)
			} else {
				log.Printf("rag: stored log embedding for failure id=%d", failureID)
			}
		}

		// Extract and record flaky test names from the log.
		testNames := extractTestNames(rawLog.Content)
		for _, name := range testNames {
			if err := db.RecordFlakyTest(ctx, evt.Repo, name, failureID); err != nil {
				log.Printf("store: record flaky test error: %v", err)
			}
		}

		// Check for flaky tests and include in the card.
		flakyTests, _ := db.FlakyTests(ctx, evt.Repo, 3, 7)
		if len(flakyTests) > 0 {
			var names []string
			for _, ft := range flakyTests {
				names = append(names, fmt.Sprintf("`%s` (%d× in 7d)", ft.TestName, ft.FailCount))
			}
			log.Printf("store: flaky tests detected: %s", strings.Join(names, ", "))
		}
	}

	blocks := buildDiagnosisCard(evt, payload, result)
	_, _, _, err = api.UpdateMessageContext(ctx, channelID, ts, slack.MsgOptionBlocks(blocks...))
	if err != nil {
		log.Printf("slack update error: %v", err)
	}
	log.Printf("diagnosis posted for run %s: category=%s confidence=%.0f%%", payload.RunID, result.Category, result.Confidence*100)
}

// stripTimestamps removes GitHub Actions per-line timestamps so that
// identical test output across different runs produces the same embedding.
// GitHub Actions format: "2026-06-13T18:25:47.1234567Z <content>"
func stripTimestamps(log string) string {
	var sb strings.Builder
	for _, line := range strings.Split(log, "\n") {
		// Timestamps are exactly: YYYY-MM-DDTHH:MM:SS.fffffffZ (29 chars + space)
		if len(line) > 30 && line[4] == '-' && line[10] == 'T' && line[19] == '.' && line[27] == 'Z' {
			line = line[29:]
		}
		sb.WriteString(line)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// extractTestNames pulls Go test names (TestXxx) from log output.
var testNameRe = regexp.MustCompile(`(?m)(?:FAIL|PASS):\s+(Test\w+)`)

func extractTestNames(log string) []string {
	matches := testNameRe.FindAllStringSubmatch(log, -1)
	seen := map[string]bool{}
	var names []string
	for _, m := range matches {
		if !seen[m[1]] {
			seen[m[1]] = true
			names = append(names, m[1])
		}
	}
	return names
}

// formatAge returns a human-readable age string (e.g. "2h ago", "3d ago").
func formatAge(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

func buildDiagnosisCard(evt events.CanonicalEvent, payload events.PipelineFailedPayload, result diagnosis.Result) []slack.Block {
	confidencePct := int(result.Confidence * 100)

	categoryEmoji := map[string]string{
		"code_defect":       ":bug:",
		"test_failure":      ":test_tube:",
		"flaky_test":        ":game_die:",
		"dependency":        ":package:",
		"infra_environment": ":cloud:",
		"config_secrets":    ":key:",
		"timeout_resource":  ":stopwatch:",
	}
	emoji := categoryEmoji[result.Category]
	if emoji == "" {
		emoji = ":warning:"
	}

	sha := payload.CommitSHA
	if len(sha) > 7 {
		sha = sha[:7]
	}

	header := slack.NewHeaderBlock(
		slack.NewTextBlockObject("plain_text", ":red_circle: Pipeline Failed", true, false),
	)

	contextBlock := slack.NewContextBlock("", []slack.MixedElement{
		slack.NewTextBlockObject("mrkdwn",
			fmt.Sprintf("*%s* · `%s` · `%s` — %s", evt.Repo, payload.WorkflowName, payload.Branch, payload.CommitMsg),
			false, false),
	}...)

	summary := slack.NewSectionBlock(
		slack.NewTextBlockObject("mrkdwn",
			fmt.Sprintf("*Summary*\n%s", result.Summary),
			false, false),
		nil, nil,
	)

	rootCause := slack.NewSectionBlock(
		slack.NewTextBlockObject("mrkdwn",
			fmt.Sprintf("*Root Cause* %s `%s` · confidence %d%%\n%s", emoji, result.Category, confidencePct, result.RootCause),
			false, false),
		nil, nil,
	)

	nextStep := slack.NewSectionBlock(
		slack.NewTextBlockObject("mrkdwn",
			fmt.Sprintf("*Next Step*\n%s", result.NextStep),
			false, false),
		nil, nil,
	)

	actions := slack.NewActionBlock("",
		slack.NewButtonBlockElement("rerun_job", payload.RunID,
			slack.NewTextBlockObject("plain_text", ":repeat: Re-run", true, false)),
		slack.NewButtonBlockElement("view_run", payload.RunURL,
			slack.NewTextBlockObject("plain_text", ":github: View Run", true, false)),
	)

	divider := slack.NewDividerBlock()

	return []slack.Block{header, contextBlock, divider, summary, rootCause, nextStep, divider, actions}
}

func handlePROpened(ctx context.Context, evt events.CanonicalEvent, p provider.Provider, ai *diagnosis.Client, api *slack.Client, cfg config) {
	payload, ok := evt.Payload.(events.PRPayload)
	if !ok {
		return
	}

	log.Printf("pr: summarising PR #%d in %s", payload.Number, evt.Repo)

	diff, err := p.FetchDiff(ctx, payload.Number, evt.Repo)
	if err != nil {
		log.Printf("pr: fetch diff error: %v", err)
		api.PostMessageContext(ctx, cfg.slackPRChannel, slack.MsgOptionText(
			fmt.Sprintf(":pr: *PR #%d opened* — `%s`\n<%s|View PR> (could not fetch diff: %v)", payload.Number, payload.Title, payload.URL, err),
			false,
		))
		return
	}

	result, err := ai.SummarizePR(ctx, diagnosis.PRSummaryRequest{
		PRNumber:    payload.Number,
		Title:       payload.Title,
		Author:      payload.Author,
		Repo:        evt.Repo,
		BaseBranch:  payload.BaseBranch,
		DiffContent: diff.Content,
	})
	if err != nil {
		log.Printf("pr: summarise error: %v", err)
		api.PostMessageContext(ctx, cfg.slackPRChannel, slack.MsgOptionText(
			fmt.Sprintf(":pr: *PR #%d opened* — `%s` by @%s\n<%s|View PR>", payload.Number, payload.Title, payload.Author, payload.URL),
			false,
		))
		return
	}

	blocks := buildPRCard(evt, payload, diff, result)
	_, _, err = api.PostMessageContext(ctx, cfg.slackPRChannel,
		slack.MsgOptionBlocks(blocks...),
		slack.MsgOptionEnableLinkUnfurl(),
	)
	if err != nil {
		log.Printf("pr: slack post error: %v", err)
	}
	log.Printf("pr: posted summary for PR #%d risk=%s files=%d +%d -%d", payload.Number, result.RiskLevel, diff.FilesChanged, diff.Additions, diff.Deletions)
}

func buildPRCard(evt events.CanonicalEvent, payload events.PRPayload, diff provider.Diff, result diagnosis.PRSummaryResult) []slack.Block {
	riskEmoji := map[string]string{
		"low":    ":white_check_mark:",
		"medium": ":warning:",
		"high":   ":red_circle:",
	}
	emoji := riskEmoji[result.RiskLevel]
	if emoji == "" {
		emoji = ":question:"
	}

	header := slack.NewHeaderBlock(
		slack.NewTextBlockObject("plain_text", ":git-pull-request: Pull Request Opened", true, false),
	)

	// Top context: repo · branch · PR number · author · stats · View PR link
	contextBlock := slack.NewContextBlock("", []slack.MixedElement{
		slack.NewTextBlockObject("mrkdwn",
			fmt.Sprintf("*%s* · `%s` → `main` · *#%d* %s · by @%s · `%d` files `+%d` `-%d` · <%s|View PR>",
				evt.Repo, payload.BaseBranch, payload.Number, payload.Title,
				payload.Author, diff.FilesChanged, diff.Additions, diff.Deletions, payload.URL),
			false, false),
	}...)

	// Plain URL in a section triggers Slack's GitHub unfurl preview card.
	urlBlock := slack.NewSectionBlock(
		slack.NewTextBlockObject("mrkdwn", payload.URL, false, false),
		nil, nil,
	)

	summary := slack.NewSectionBlock(
		slack.NewTextBlockObject("mrkdwn",
			fmt.Sprintf("*Summary*\n%s", result.Summary),
			false, false),
		nil, nil,
	)

	divider := slack.NewDividerBlock()

	var flagLines []string
	for _, f := range result.RiskFlags {
		flagLines = append(flagLines, "• "+f)
	}
	riskBlock := slack.NewSectionBlock(
		slack.NewTextBlockObject("mrkdwn",
			fmt.Sprintf("*Risk* %s `%s`\n%s", emoji, result.RiskLevel, strings.Join(flagLines, "\n")),
			false, false),
		nil, nil,
	)

	var checkLines []string
	for _, c := range result.Checklist {
		checkLines = append(checkLines, "☐ "+c)
	}
	checkBlock := slack.NewSectionBlock(
		slack.NewTextBlockObject("mrkdwn",
			fmt.Sprintf("*Review Checklist*\n%s", strings.Join(checkLines, "\n")),
			false, false),
		nil, nil,
	)

	return []slack.Block{header, contextBlock, urlBlock, divider, summary, riskBlock, checkBlock}
}

func updateSlackMessage(ctx context.Context, api *slack.Client, channel, ts, text string) {
	api.UpdateMessageContext(ctx, channel, ts, slack.MsgOptionText(text, false))
}

func handleSocketEvent(ctx context.Context, evt socketmode.Event, client *socketmode.Client, api *slack.Client, p provider.Provider, cfg config, ready chan struct{}) {
	switch evt.Type {
	case socketmode.EventTypeConnecting:
		log.Println("slack: connecting...")
	case socketmode.EventTypeConnected:
		log.Println("slack: connected via Socket Mode")
	case socketmode.EventTypeHello:
		log.Println("slack: hello received — ready")
		select {
		case <-ready:
		default:
			close(ready)
		}
	case socketmode.EventTypeSlashCommand:
		client.Ack(*evt.Request)
		handleSlashCommand(ctx, evt, api, p, cfg)
	case socketmode.EventTypeEventsAPI:
		client.Ack(*evt.Request)
		log.Printf("slack event received: %T", evt.Data)
	}
}

func handleSlashCommand(ctx context.Context, evt socketmode.Event, api *slack.Client, p provider.Provider, cfg config) {
	cmd, ok := evt.Data.(slack.SlashCommand)
	if !ok {
		return
	}
	switch cmd.Text {
	case "status":
		api.PostMessage(cmd.ChannelID, slack.MsgOptionText(
			fmt.Sprintf(":white_check_mark: PipelineCopilot is running. Provider: *%s*", p.Name()),
			false,
		))
	default:
		api.PostMessage(cmd.ChannelID, slack.MsgOptionText(
			"Unknown command. Try `/pipeline status`.", false,
		))
	}
}

type config struct {
	provider            string
	slackBotToken       string
	slackAppToken       string
	slackSigningSecret  string
	slackChannel        string
	slackPRChannel      string
	webhookPort         string
	githubWebhookSecret string
	githubToken         string
	aiServiceURL        string
	databaseURL         string
}

func loadConfig() config {
	return config{
		provider:            getenv("PROVIDER", "fake"),
		slackBotToken:       requireenv("SLACK_BOT_TOKEN"),
		slackAppToken:       requireenv("SLACK_APP_TOKEN"),
		slackSigningSecret:  requireenv("SLACK_SIGNING_SECRET"),
		slackChannel:        getenv("SLACK_CHANNEL", "general"),
		slackPRChannel:      getenv("SLACK_PR_CHANNEL", "pr-reviews"),
		webhookPort:         getenv("WEBHOOK_PORT", "8080"),
		githubWebhookSecret: os.Getenv("GITHUB_WEBHOOK_SECRET"),
		githubToken:         os.Getenv("GITHUB_TOKEN"),
		aiServiceURL:        getenv("AI_SERVICE_URL", "http://localhost:8000"),
		databaseURL:         getenv("DATABASE_URL", "postgres://pipelinecopilot:pipelinecopilot@localhost:5432/pipelinecopilot?sslmode=disable"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func requireenv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}
