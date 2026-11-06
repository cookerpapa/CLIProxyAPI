package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

// nativeResponsesRequests uses actual route evidence, including payload-dependent
// plugin translators, instead of forwarding a different protocol as raw JSON.
func (e *OpenAICompatExecutor) nativeResponsesRequests(ctx context.Context, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, model string, original []byte, stream bool) ([]byte, []byte, bool, error) {
	from, to := opts.SourceFormat, sdktranslator.FormatOpenAIResponse
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	if responseFormat != to && !sdktranslator.HasPluginHooks() {
		handled := sdktranslator.HasNonStreamResponseTransformer(responseFormat, to)
		if stream {
			handled = sdktranslator.HasStreamResponseTransformer(responseFormat, to)
		}
		if !handled {
			return nil, nil, false, openAICompatNativeResponseError{statusErr{code: http.StatusBadRequest, msg: "native Responses provider has no response translation for this client protocol"}}
		}
	}
	translate := func(body []byte) (sdktranslator.RequestEnvelope, bool) {
		return sdktranslator.TranslateRequestEnvelopeChecked(ctx, from, to, sdktranslator.RequestEnvelope{
			Format: from, Model: model, Stream: stream, Body: body,
		})
	}
	working, handled := translate(req.Payload)
	if working.Err != nil {
		return nil, nil, false, openAICompatNativeResponseError{statusErr{code: http.StatusBadRequest, msg: working.Err.Error()}}
	}
	if !handled {
		return nil, nil, false, openAICompatNativeResponseError{statusErr{code: http.StatusBadRequest, msg: "native Responses provider has no request translation for this client protocol"}}
	}
	if !json.Valid(working.Body) {
		return nil, nil, false, openAICompatNativeResponseError{statusErr{code: http.StatusBadRequest, msg: "native Responses translation returned invalid JSON"}}
	}
	if bytes.Equal(original, req.Payload) {
		return bytes.Clone(working.Body), working.Body, working.ConfigurationUpdatesChanged, nil
	}
	baseline, _ := translate(original)
	return baseline.Body, working.Body, working.ConfigurationUpdatesChanged, nil
}
