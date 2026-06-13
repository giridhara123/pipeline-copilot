package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"

	"github.com/giridhara123/pipeline-copilot/internal/diagnosis"
	"github.com/giridhara123/pipeline-copilot/internal/events"
	fakeprovider "github.com/giridhara123/pipeline-copilot/internal/provider/fake"
	githubprovider "github.com/giridhara123/pipeline-copilot/internal/provider/github"
	"github.com/giridhara123/pipeline-copilot/internal/provider"
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

	api := slack.New(
		cfg.slackBotToken,
		slack.OptionAppLevelToken(cfg.slackAppToken),
	)
	client := socketmode.New(api)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{})

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("shutting down...")
		cancel()
	}()

	// eventHandler is called for every valid canonical event from any source.
	eventHandler := func(ctx context.Context, evt events.CanonicalEvent) {
		switch evt.Type {
		case events.EventPipelineFailed:
			handlePipelineFailed(ctx, evt, p, ai, api, cfg)
		default:
			log.Printf("unhandled event type: %s", evt.Type)
		}
	}

	// HTTP server for GitHub webhooks.
	webhookServer := webhook.NewServer(p, eventHandler)
	go func() {
		log.Printf("webhook server listening on :%s", cfg.webhookPort)
		if err := http.ListenAndServe(":"+cfg.webhookPort, webhookServer); err != nil {
			log.Fatalf("webhook server error: %v", err)
		}
	}()

	// Slack Socket Mode event loop.
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

	// In fake mode, fire a synthetic failure after Slack is ready.
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

func handlePipelineFailed(ctx context.Context, evt events.CanonicalEvent, p provider.Provider, ai *diagnosis.Client, api *slack.Client, cfg config) {
	payload, ok := evt.Payload.(events.PipelineFailedPayload)
	if !ok {
		return
	}

	log.Printf("diagnosing failure: run=%s repo=%s", payload.RunID, evt.Repo)

	// Post an immediate "investigating" placeholder.
	// Capture channelID from the response — chat.update requires the ID, not the name.
	placeholderText := fmt.Sprintf(":hourglass: *Investigating pipeline failure* in `%s` — `%s` on `%s`...", evt.Repo, payload.WorkflowName, payload.Branch)
	channelID, ts, err := api.PostMessageContext(ctx, cfg.slackChannel, slack.MsgOptionText(placeholderText, false))
	if err != nil {
		log.Printf("slack placeholder post error: %v", err)
	}

	// Fetch logs from the provider.
	runRef := evt.Repo + "/" + payload.RunID
	rawLog, err := p.FetchLogs(ctx, runRef)
	if err != nil {
		log.Printf("fetch logs error: %v", err)
		updateSlackMessage(ctx, api, channelID, ts,
			fmt.Sprintf(":red_circle: *Pipeline Failed* — `%s`\nCould not fetch logs: %v\n<%s|View Run>", payload.WorkflowName, err, payload.RunURL))
		return
	}

	// Call the AI service.
	result, err := ai.Diagnose(ctx, diagnosis.Request{
		RunID:        payload.RunID,
		Repo:         evt.Repo,
		Branch:       payload.Branch,
		CommitSHA:    payload.CommitSHA,
		CommitMsg:    payload.CommitMsg,
		WorkflowName: payload.WorkflowName,
		LogContent:   rawLog.Content,
	})
	if err != nil {
		log.Printf("diagnosis error: %v", err)
		updateSlackMessage(ctx, api, channelID, ts,
			fmt.Sprintf(":red_circle: *Pipeline Failed* — `%s`\nDiagnosis unavailable.\n<%s|View Run>", payload.WorkflowName, payload.RunURL))
		return
	}

	// Build a Block Kit diagnosis card and update the placeholder.
	blocks := buildDiagnosisCard(evt, payload, result)
	_, _, _, err = api.UpdateMessageContext(ctx, channelID, ts, slack.MsgOptionBlocks(blocks...))
	if err != nil {
		log.Printf("slack update error: %v", err)
	}
	log.Printf("diagnosis posted for run %s: category=%s confidence=%.0f%%", payload.RunID, result.Category, result.Confidence*100)
}

func buildDiagnosisCard(evt events.CanonicalEvent, payload events.PipelineFailedPayload, result diagnosis.Result) []slack.Block {
	confidencePct := int(result.Confidence * 100)

	categoryEmoji := map[string]string{
		"code_defect":        ":bug:",
		"test_failure":       ":test_tube:",
		"flaky_test":         ":game_die:",
		"dependency":         ":package:",
		"infra_environment":  ":cloud:",
		"config_secrets":     ":key:",
		"timeout_resource":   ":stopwatch:",
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

	context := slack.NewContextBlock("", []slack.MixedElement{
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

	return []slack.Block{header, context, divider, summary, rootCause, nextStep, divider, actions}
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
	webhookPort         string
	githubWebhookSecret string
	githubToken         string
	aiServiceURL        string
}

func loadConfig() config {
	return config{
		provider:            getenv("PROVIDER", "fake"),
		slackBotToken:       requireenv("SLACK_BOT_TOKEN"),
		slackAppToken:       requireenv("SLACK_APP_TOKEN"),
		slackSigningSecret:  requireenv("SLACK_SIGNING_SECRET"),
		slackChannel:        getenv("SLACK_CHANNEL", "general"),
		webhookPort:         getenv("WEBHOOK_PORT", "8080"),
		githubWebhookSecret: os.Getenv("GITHUB_WEBHOOK_SECRET"),
		githubToken:         os.Getenv("GITHUB_TOKEN"),
		aiServiceURL:        getenv("AI_SERVICE_URL", "http://localhost:8000"),
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
