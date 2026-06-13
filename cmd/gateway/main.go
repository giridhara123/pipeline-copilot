package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"

	"github.com/giridhara123/pipeline-copilot/internal/events"
	githubprovider "github.com/giridhara123/pipeline-copilot/internal/provider/github"
	fakeprovider "github.com/giridhara123/pipeline-copilot/internal/provider/fake"
	"github.com/giridhara123/pipeline-copilot/internal/provider"
)

func main() {
	// Load .env file if present (ignored in production where env vars are injected).
	_ = godotenv.Load()

	cfg := loadConfig()

	// Select the provider based on the PROVIDER env var.
	var p provider.Provider
	switch cfg.provider {
	case "github":
		p = githubprovider.New(cfg.githubWebhookSecret, cfg.githubToken)
		log.Println("provider: github")
	default:
		p = fakeprovider.New()
		log.Println("provider: fake (set PROVIDER=github for real GitHub events)")
	}

	// Connect to Slack via Socket Mode.
	api := slack.New(
		cfg.slackBotToken,
		slack.OptionAppLevelToken(cfg.slackAppToken),
	)
	client := socketmode.New(api)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ready is closed once Slack sends the hello — safe to post after this.
	ready := make(chan struct{})

	// Graceful shutdown on SIGINT/SIGTERM.
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("shutting down...")
		cancel()
	}()

	// Event handler loop.
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

	// In fake/dev mode, fire a synthetic failure event only after Slack is ready.
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
			postEventToSlack(ctx, api, cfg.slackChannel, evt)
		}()
	}

	if err := client.RunContext(ctx); err != nil {
		log.Fatalf("socket mode error: %v", err)
	}
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
		case <-ready: // already closed, do nothing
		default:
			close(ready)
		}
	case socketmode.EventTypeSlashCommand:
		client.Ack(*evt.Request)
		handleSlashCommand(ctx, evt, api, p, cfg)
	case socketmode.EventTypeEventsAPI:
		client.Ack(*evt.Request)
		log.Printf("slack event received: %T", evt.Data)
	default:
		// Ignore other event types silently.
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
			"Unknown command. Try `/pipeline status`.",
			false,
		))
	}
}

func postEventToSlack(ctx context.Context, api *slack.Client, channel string, evt events.CanonicalEvent) {
	var text string

	switch evt.Type {
	case events.EventPipelineFailed:
		p, _ := evt.Payload.(events.PipelineFailedPayload)
		text = fmt.Sprintf(
			":red_circle: *Pipeline Failed* — `%s`\n*Repo:* %s\n*Branch:* `%s`\n*Commit:* `%s` — %s\n*Failed Step:* %s\n<%s|View Run>",
			p.WorkflowName, evt.Repo, p.Branch, p.CommitSHA[:7], p.CommitMsg, p.FailedStep, p.RunURL,
		)
	default:
		text = fmt.Sprintf("Event received: %s from %s", evt.Type, evt.Provider)
	}

	_, _, err := api.PostMessageContext(ctx, channel, slack.MsgOptionText(text, false))
	if err != nil {
		log.Printf("slack post error: %v", err)
		return
	}
	log.Printf("posted event %s to slack channel %s", evt.Type, channel)
}

type config struct {
	provider            string
	slackBotToken       string
	slackAppToken       string
	slackSigningSecret  string
	slackChannel        string
	githubWebhookSecret string
	githubToken         string
}

func loadConfig() config {
	return config{
		provider:            getenv("PROVIDER", "fake"),
		slackBotToken:       requireenv("SLACK_BOT_TOKEN"),
		slackAppToken:       requireenv("SLACK_APP_TOKEN"),
		slackSigningSecret:  requireenv("SLACK_SIGNING_SECRET"),
		slackChannel:        getenv("SLACK_CHANNEL", "general"),
		githubWebhookSecret: os.Getenv("GITHUB_WEBHOOK_SECRET"),
		githubToken:         os.Getenv("GITHUB_TOKEN"),
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
