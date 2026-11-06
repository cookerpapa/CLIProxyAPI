package management

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestConfigV8NativeWireAPIRetainedAndInvalidWriteRejected(t *testing.T) {
	gin.SetMode(gin.TestMode)
	path := filepath.Join(t.TempDir(), "config.yaml")
	raw := []byte("config-version: 8\nserver:\n  port: 8317\napi-keys:\n  openai-compatibility:\n    - name: native\n      base-url: https://example.invalid/v1\n      keys: []\n      wire-api: responses\n")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: cfg, configFilePath: path}
	router := gin.New()
	router.GET("/v8/management/config/*path", h.ConfigV8)
	router.PUT("/v8/management/config/*path", h.ConfigV8)
	get := httptest.NewRecorder()
	router.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v8/management/config/api-keys/openai-compatibility", nil))
	if get.Code != 200 || !strings.Contains(get.Body.String(), `"wire-api":"responses"`) {
		t.Fatalf("protocol omitted: %d %s", get.Code, get.Body.String())
	}
	put := httptest.NewRecorder()
	router.ServeHTTP(put, httptest.NewRequest(http.MethodPut, "/v8/management/config/api-keys/openai-compatibility", strings.NewReader(`[{"name":"native","base-url":"https://example.invalid/v1","keys":[],"wire-api":"invalid"}]`)))
	if put.Code != 422 {
		t.Fatalf("invalid protocol status=%d", put.Code)
	}
	saved, _ := os.ReadFile(path)
	if string(saved) != string(raw) || h.cfg.OpenAICompatibility[0].WireAPI != "responses" {
		t.Fatal("invalid write changed live or persisted protocol")
	}
}
