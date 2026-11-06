package executor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

func nativeResponsesExecutor(serverURL string) (*OpenAICompatExecutor, *cliproxyauth.Auth) {
	cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{
		Name:    "deepseek-native",
		BaseURL: serverURL,
		WireAPI: "responses",
	}}}
	return NewOpenAICompatExecutor("deepseek-native", cfg), &cliproxyauth.Auth{
		Provider: "deepseek-native",
		Attributes: map[string]string{
			"base_url": serverURL,
			"api_key":  "test-key",
		},
	}
}

func TestOpenAICompatNativeResponsesCancellationClosesUpstream(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	}))
	defer server.Close()
	e, auth := nativeResponsesExecutor(server.URL)
	req, opts := nativeResponsesRequest(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result, err := e.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatal(err)
	}
	if chunk := <-result.Chunks; chunk.Err != nil {
		t.Fatal(chunk.Err)
	}
	cancel()
	settled := make(chan struct{})
	go func() {
		for range result.Chunks {
		}
		close(settled)
	}()
	for _, done := range []<-chan struct{}{closed, settled} {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("cancelled stream remained open")
		}
	}
}

func TestOpenAICompatNativeResponsesTerminalBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, events, want string
		failed             bool
	}{
		{"named-multiline", "event: response.completed\ndata: {\"type\":\"response.completed\",\ndata: \"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n", "event: response.completed", false},
		{"incomplete", "data: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"r\",\"status\":\"incomplete\",\"output\":[]}}\n\n", "response.incomplete", false},
		{"provider-failure", "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"r\",\"status\":\"failed\",\"error\":{\"code\":\"fixture_rejected\"}}}\n\n", "fixture_rejected", false},
		{"done-before-terminal", "data: [DONE]\n\n", "terminal event", true},
		{"invalid-json", "data: {\"type\":\"response.completed\"\n\n", "incomplete SSE", true},
		{"contradictory-status", "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"failed\"}}\n\n", "invalid status", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, tc.events)
			}))
			defer server.Close()
			executor, auth := nativeResponsesExecutor(server.URL)
			req, opts := nativeResponsesRequest(true)
			result, err := executor.ExecuteStream(context.Background(), auth, req, opts)
			if err != nil {
				t.Fatal(err)
			}
			var output strings.Builder
			var failure error
			for chunk := range result.Chunks {
				output.Write(chunk.Payload)
				if chunk.Err != nil {
					failure = chunk.Err
				}
			}
			if tc.failed {
				var scoped cliproxyexecutor.RequestScopedError
				if failure == nil || !strings.Contains(failure.Error(), tc.want) || !errors.As(failure, &scoped) || !scoped.IsRequestScoped() {
					t.Fatalf("failure did not preserve request scope: %v", failure)
				}
			} else if failure != nil || !strings.Contains(output.String(), tc.want) {
				t.Fatalf("native event lost: error=%v output=%s", failure, output.String())
			}
		})
	}
}

func TestOpenAICompatNativeResponsesPreservesOpaqueInputAndExecutionMode(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "stream"}[stream], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if ojson.GetBytesNoCopy(body, "input.0.id").String() != "opaque-id" || ojson.GetBytesNoCopy(body, "input.0.encrypted_content").String() != "opaque-value" || ojson.GetBytesNoCopy(body, "stream").Bool() != stream {
					t.Errorf("native replay or stream mode changed: %s", body)
				}
				if ojson.GetBytesNoCopy(body, "stream_options").Exists() {
					t.Error("chat-only usage option injected")
				}
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n")
				} else {
					io.WriteString(w, `{"id":"r","status":"completed","output":[]}`)
				}
			}))
			defer server.Close()
			e, auth := nativeResponsesExecutor(server.URL)
			req, opts := nativeResponsesRequest(!stream)
			req.Payload = []byte(`{"model":"deepseek-v4-flash","input":[{"type":"reasoning","id":"opaque-id","encrypted_content":"opaque-value"}]}`)
			opts.OriginalRequest = req.Payload
			if stream {
				result, err := e.ExecuteStream(context.Background(), auth, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			} else if _, err := e.Execute(context.Background(), auth, req, opts); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenAICompatNativeResponsesRejectsUnknownClientBeforeDispatch(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer server.Close()
	e, auth := nativeResponsesExecutor(server.URL)
	for _, stream := range []bool{false, true} {
		req, opts := nativeResponsesRequest(stream)
		opts.SourceFormat = sdktranslator.Format("unknown-fixture")
		opts.ResponseFormat = sdktranslator.Format("unknown-fixture")
		var err error
		if stream {
			_, err = e.ExecuteStream(context.Background(), auth, req, opts)
		} else {
			_, err = e.Execute(context.Background(), auth, req, opts)
		}
		var scoped cliproxyexecutor.RequestScopedError
		if err == nil || !errors.As(err, &scoped) || !scoped.IsRequestScoped() {
			t.Fatalf("missing route was not refused: %v", err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("unsupported protocol reached provider")
	}
}

func nativeResponsesRequest(stream bool) (cliproxyexecutor.Request, cliproxyexecutor.Options) {
	payload := []byte(`{"model":"deepseek-v4-flash","input":"search","tools":[{"type":"web_search"}],"stream":` + map[bool]string{true: "true", false: "false"}[stream] + `}`)
	return cliproxyexecutor.Request{
			Model:   "deepseek-v4-flash",
			Payload: payload,
		}, cliproxyexecutor.Options{
			Stream:          stream,
			SourceFormat:    sdktranslator.FormatOpenAIResponse,
			ResponseFormat:  sdktranslator.FormatOpenAIResponse,
			OriginalRequest: payload,
		}
}

func TestOpenAICompatNativeResponsesPreservesHostedTools(t *testing.T) {
	var upstreamBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("path = %q, want /responses", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("authorization = %q", got)
		}
		upstreamBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp-1","object":"response","status":"completed","model":"deepseek-v4-flash","output":[{"id":"ws-1","type":"web_search_call","status":"completed","action":{"type":"search","query":"search"}},{"id":"msg-1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"found"}]}],"usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}`))
	}))
	defer server.Close()

	executor, auth := nativeResponsesExecutor(server.URL)
	req, opts := nativeResponsesRequest(false)
	response, err := executor.Execute(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := ojson.GetBytes(upstreamBody, "tools.0.type").String(); got != "web_search" {
		t.Fatalf("hosted tool = %q, body=%s", got, upstreamBody)
	}
	if got := ojson.GetBytes(response.Payload, "output.0.type").String(); got != "web_search_call" {
		t.Fatalf("response output = %q, body=%s", got, response.Payload)
	}
}

func TestOpenAICompatNativeResponsesStreamsSemanticEventsWithoutDoneMarker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("path = %q, want /responses", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if got := ojson.GetBytes(body, "tools.0.type").String(); got != "web_search" {
			t.Fatalf("hosted tool = %q, body=%s", got, body)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_item.done\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.output_item.done","output_index":0,"item":{"id":"ws-1","type":"web_search_call","status":"completed","action":{"type":"search","query":"search"}}}` + "\n\n"))
		_, _ = w.Write([]byte("event: response.output_text.delta\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.output_text.delta","output_index":1,"content_index":0,"delta":"found"}` + "\n\n"))
		_, _ = w.Write([]byte("event: response.completed\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"id":"resp-1","status":"completed","model":"deepseek-v4-flash","output":[],"usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}` + "\n\n"))
	}))
	defer server.Close()

	executor, auth := nativeResponsesExecutor(server.URL)
	req, opts := nativeResponsesRequest(true)
	result, err := executor.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}
	var output strings.Builder
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream error: %v", chunk.Err)
		}
		output.Write(chunk.Payload)
	}
	if !strings.Contains(output.String(), `"type":"web_search_call"`) {
		t.Fatalf("web_search_call missing: %s", output.String())
	}
	if !strings.Contains(output.String(), `"type":"response.completed"`) {
		t.Fatalf("response.completed missing: %s", output.String())
	}
	if strings.Contains(output.String(), "[DONE]") {
		t.Fatalf("native Responses stream synthesized a Chat Completions marker: %s", output.String())
	}
}

func TestOpenAICompatNativeResponsesRejectsEOFBeforeTerminalEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"response.output_text.delta","delta":"partial"}` + "\n\n"))
	}))
	defer server.Close()

	executor, auth := nativeResponsesExecutor(server.URL)
	req, opts := nativeResponsesRequest(true)
	result, err := executor.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}
	var streamErr error
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if streamErr == nil || !strings.Contains(streamErr.Error(), "terminal event") {
		t.Fatalf("stream error = %v, want missing terminal event", streamErr)
	}
}
