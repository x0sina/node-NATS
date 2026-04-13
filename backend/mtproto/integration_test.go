package mtproto

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/pasarguard/node/common"
	"github.com/pasarguard/node/config"
)

func TestMTProtoBackendLifecycleAndSync(t *testing.T) {
	t.Setenv(helperProcessEnv, "1")
	t.Setenv("MTPROTO_HELPER_HEALTH_DELAY_MS", "200")

	executablePath, err := os.Executable()
	if err != nil {
		t.Fatalf("failed to resolve test executable: %v", err)
	}

	tempDir := t.TempDir()
	cfg := config.NewTestConfig(tempDir, uuid.New())
	cfg.TelemtExecutablePath = executablePath
	cfg.LogBufferSize = 64

	mtConfig, err := NewConfig(`{
		"server": {"port": 443},
		"censorship": {"tls_domain": "example.com"}
	}`)
	if err != nil {
		t.Fatalf("NewConfig returned error: %v", err)
	}

	startupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	backend, err := New(startupCtx, cfg, mtConfig, []*common.User{
		makeMTProtoUser("100.alice", mtConfig.InboundTag, "00112233445566778899aabbccddeeff"),
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer backend.Shutdown()

	if !backend.Started() {
		t.Fatalf("expected backend to be started")
	}

	renderedConfigPath := filepath.Join(tempDir, "telemt", "config.toml")
	renderedConfig, err := os.ReadFile(renderedConfigPath)
	if err != nil {
		t.Fatalf("failed to read rendered telemt config: %v", err)
	}
	if !strings.Contains(string(renderedConfig), `"100.alice" = "00112233445566778899aabbccddeeff"`) {
		t.Fatalf("startup config missing initial user:\n%s", string(renderedConfig))
	}

	select {
	case line := <-backend.Logs():
		if !strings.Contains(line, "helper started") {
			t.Fatalf("expected helper startup log, got %q", line)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for helper startup log")
	}

	if err := backend.UpdateUsers(context.Background(), []*common.User{
		makeMTProtoUser("200.bob", mtConfig.InboundTag, "11112222333344445555666677778888"),
	}); err != nil {
		t.Fatalf("UpdateUsers create returned error: %v", err)
	}
	if _, err := backend.apiClient.GetUser(context.Background(), "200.bob"); err != nil {
		t.Fatalf("expected created user to be visible through helper API: %v", err)
	}

	if err := backend.UpdateUsers(context.Background(), []*common.User{
		makeMTProtoUser("100.alice", mtConfig.InboundTag, "8899aabbccddeeff0011223344556677"),
	}); err != nil {
		t.Fatalf("UpdateUsers patch returned error: %v", err)
	}
	if backend.desired["100.alice"].Secret != "8899aabbccddeeff0011223344556677" {
		t.Fatalf("expected desired state to be updated after patch, got %#v", backend.desired["100.alice"])
	}

	if err := backend.UpdateUsers(context.Background(), []*common.User{
		{Email: "200.bob", Proxies: &common.Proxy{}},
	}); err != nil {
		t.Fatalf("UpdateUsers delete returned error: %v", err)
	}
	if _, err := backend.apiClient.GetUser(context.Background(), "200.bob"); err == nil {
		t.Fatal("expected deleted user lookup to fail")
	} else {
		var apiErr *apiStatusError
		if !errors.As(err, &apiErr) || !apiErr.NotFound() {
			t.Fatalf("expected not found after delete, got %v", err)
		}
	}

	if err := backend.SyncUsers(context.Background(), []*common.User{
		makeMTProtoUser("300.carol", mtConfig.InboundTag, "abcdefabcdefabcdefabcdefabcdefab"),
	}); err != nil {
		t.Fatalf("SyncUsers returned error: %v", err)
	}
	if _, err := backend.apiClient.GetUser(context.Background(), "300.carol"); err != nil {
		t.Fatalf("expected synced user to be visible after reload: %v", err)
	}
	if _, err := backend.apiClient.GetUser(context.Background(), "100.alice"); err == nil {
		t.Fatal("expected removed user to disappear after full reload")
	}

	if err := backend.UpdateUsers(context.Background(), []*common.User{
		{Email: "300.carol", Proxies: &common.Proxy{}},
	}); err != nil {
		t.Fatalf("UpdateUsers last delete returned error: %v", err)
	}
	if len(backend.desired) != 0 {
		t.Fatalf("expected desired state to be empty after deleting last user, got %d users", len(backend.desired))
	}

	renderedConfig, err = os.ReadFile(renderedConfigPath)
	if err != nil {
		t.Fatalf("failed to read rendered telemt config after sentinel rewrite: %v", err)
	}
	if !strings.Contains(string(renderedConfig), backend.sentinel.Username) {
		t.Fatalf("expected sentinel user in rendered config after deleting last user:\n%s", string(renderedConfig))
	}
	if strings.Contains(string(renderedConfig), "300.carol") {
		t.Fatalf("expected deleted user to be absent from rendered config:\n%s", string(renderedConfig))
	}

	previousPID := backend.process.PID()
	reloadFailureMarker := filepath.Join(tempDir, "telemt", "telemt.pid.reload_fail")
	if err := os.WriteFile(reloadFailureMarker, []byte("1"), 0o644); err != nil {
		t.Fatalf("failed to create reload failure marker: %v", err)
	}
	defer os.Remove(reloadFailureMarker)

	if err := backend.SyncUsers(context.Background(), []*common.User{
		makeMTProtoUser("400.dave", mtConfig.InboundTag, "1234567890abcdef1234567890abcdef"),
	}); err != nil {
		t.Fatalf("SyncUsers restart fallback returned error: %v", err)
	}
	if backend.process.PID() == previousPID {
		t.Fatalf("expected reload failure to trigger process restart")
	}
	if _, err := backend.apiClient.GetUser(context.Background(), "400.dave"); err != nil {
		t.Fatalf("expected user after restart fallback: %v", err)
	}
}

func makeMTProtoUser(email, inboundTag, secret string) *common.User {
	return &common.User{
		Email:    email,
		Inbounds: []string{inboundTag},
		Proxies: &common.Proxy{
			Mtproto: &common.Mtproto{
				Secret: secret,
			},
		},
	}
}
