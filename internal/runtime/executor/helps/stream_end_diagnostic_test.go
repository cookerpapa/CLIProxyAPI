package helps

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
)

func TestStreamEndKind(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		err  error
		want string
	}{
		{nil, "clean_eof_before_terminal"},
		{fmt.Errorf("body: %w", io.ErrUnexpectedEOF), "unexpected_eof"},
		{http2.StreamError{Code: http2.ErrCodeCancel}, "http2_CANCEL"},
		{fmt.Errorf("read failed"), "read_error"},
	} {
		if got := streamEndKind(ctx, tt.err); got != tt.want {
			t.Fatalf("got %q, want %q", got, tt.want)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if got := streamEndKind(cancelled, nil); got != "downstream_cancelled" {
		t.Fatal(got)
	}
}

func TestStreamEndLogsExcludeBodiesAndIgnoreCancellation(t *testing.T) {
	var output bytes.Buffer
	previous := log.StandardLogger().Out
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	response := &http.Response{StatusCode: 200, Proto: "HTTP/2.0", Header: http.Header{
		"X-Request-Id": []string{"fixture-request"}, "Authorization": []string{"private-credential"},
	}}
	body := []byte(`{"error":{"code":"fixture_code","type":"provider_error","message":"private-provider-body"},"prompt":"private-prompt"}`)
	LogProviderStreamFailure(context.Background(), response, body, time.Now())
	LogIncompleteResponseStream(context.Background(), response, io.ErrUnexpectedEOF, time.Now())
	text := output.String()
	if !strings.Contains(text, "fixture_code") || !strings.Contains(text, "unexpected_eof") || strings.Contains(text, "private-") {
		t.Fatalf("diagnostic fields were not bounded: %s", text)
	}
	output.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	LogProviderStreamFailure(ctx, response, body, time.Now())
	LogIncompleteResponseStream(ctx, response, io.ErrUnexpectedEOF, time.Now())
	if output.Len() != 0 {
		t.Fatal("downstream cancellation was reported as an upstream failure")
	}
}
