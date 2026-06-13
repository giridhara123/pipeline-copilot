package webhook

import (
	"context"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/giridhara123/pipeline-copilot/internal/events"
	"github.com/giridhara123/pipeline-copilot/internal/provider"
)

// Handler is the function called when a valid canonical event arrives.
type Handler func(ctx context.Context, evt events.CanonicalEvent)

// Server listens for inbound provider webhooks on /webhook.
type Server struct {
	provider provider.Provider
	handler  Handler
}

func NewServer(p provider.Provider, h Handler) *Server {
	return &Server{provider: p, handler: h}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/webhook" || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 5<<20)) // 5 MB limit
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Store headers in lowercase so provider lookups are case-insensitive.
	headers := make(map[string]string, len(r.Header))
	for k, v := range r.Header {
		if len(v) > 0 {
			headers[strings.ToLower(k)] = v[0]
		}
	}

	// Ack immediately — GitHub expects 200 within 10 seconds.
	w.WriteHeader(http.StatusOK)

	if err := s.provider.VerifyWebhook(headers, body); err != nil {
		log.Printf("webhook: signature verification failed: %v", err)
		return
	}

	evt, err := s.provider.ParseEvent(headers, body)
	if err != nil {
		log.Printf("webhook: parse skipped: %v", err)
		return
	}

	log.Printf("webhook: received %s from %s/%s", evt.Type, evt.Provider, evt.Repo)

	// Use context.Background() — r.Context() is cancelled once we send 200.
	go s.handler(context.Background(), evt)
}
