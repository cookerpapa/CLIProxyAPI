package config

import (
	"fmt"
	"strings"
	"testing"
)

func TestOpenAICompatibilityWireAPIV8(t *testing.T) {
	for _, protocol := range []string{"", "responses", " RESPONSES ", "chat-completions", "invalid"} {
		t.Run(protocol, func(t *testing.T) {
			body := []byte(fmt.Sprintf("config-version: 8\nserver:\n  port: 8317\napi-keys:\n  openai-compatibility:\n    - name: native\n      base-url: https://example.invalid/v1\n      keys: []\n      wire-api: %q\n", protocol))
			cfg, err := ParseConfigBytes(body)
			validation := ValidateV8Config(body)
			if protocol == "invalid" {
				if err == nil || validation == nil {
					t.Fatalf("unsupported protocol accepted: load=%v validation=%v", err, validation)
				}
				return
			}
			if err != nil || validation != nil {
				t.Fatalf("valid protocol rejected: load=%v validation=%v", err, validation)
			}
			if len(cfg.OpenAICompatibility) != 1 || cfg.OpenAICompatibility[0].WireAPI != strings.ToLower(strings.TrimSpace(protocol)) {
				t.Fatalf("protocol not retained: %#v", cfg.OpenAICompatibility)
			}
		})
	}
}
