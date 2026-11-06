package config

import (
	"fmt"
	"strings"
)

// ValidateOpenAICompatibilityWireAPI rejects unsupported upstream protocols before
// configuration is adopted or a management update is persisted.
func ValidateOpenAICompatibilityWireAPI(entries []OpenAICompatibility) error {
	for i, entry := range entries {
		switch strings.ToLower(strings.TrimSpace(entry.WireAPI)) {
		case "", "chat-completions", "responses":
		default:
			return fmt.Errorf("openai-compatibility[%d].wire-api must be chat-completions or responses", i)
		}
	}
	return nil
}
