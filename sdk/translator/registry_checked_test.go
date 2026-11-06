package translator

import (
	"context"
	"testing"
)

func TestCheckedTranslationDistinguishesFallbackFromHandledPayload(t *testing.T) {
	for _, kind := range []string{"missing", "declined", "plugin", "identity"} {
		t.Run(kind, func(t *testing.T) {
			r := NewRegistry()
			from, to := Format("source"), Format("target")
			if kind == "identity" {
				to = from
			}
			if kind == "declined" || kind == "plugin" {
				r.SetPluginHooks(&fakePluginHooks{
					requestTranslateOK: kind == "plugin", responseTranslateOK: kind == "plugin",
					requestTranslateBody: []byte(`{"request":true}`), responseTranslateBody: []byte(`{"response":true}`),
				})
			}
			expected := kind == "identity" || kind == "plugin"
			_, request := r.TranslateRequestEnvelopeChecked(context.Background(), from, to, RequestEnvelope{Format: from, Body: []byte(`{}`)})
			_, stream := r.TranslateStreamChecked(context.Background(), to, from, "model", nil, nil, []byte(`{}`), nil)
			_, nonstream := r.TranslateNonStreamChecked(context.Background(), to, from, "model", nil, nil, []byte(`{}`), nil)
			if request != expected || stream != expected || nonstream != expected {
				t.Fatalf("route evidence: request=%v stream=%v nonstream=%v want=%v", request, stream, nonstream, expected)
			}
		})
	}
}
