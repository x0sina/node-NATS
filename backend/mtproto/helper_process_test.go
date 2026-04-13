package mtproto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const helperProcessEnv = "GO_WANT_MTPROTO_HELPER_PROCESS"

func TestMain(m *testing.M) {
	if os.Getenv(helperProcessEnv) == "1" {
		os.Exit(runHelperProcess())
	}
	os.Exit(m.Run())
}

type helperRuntime struct {
	mu            sync.RWMutex
	configPath    string
	pidFilePath   string
	apiListen     string
	metricsListen string
	authHeader    string
	revision      uint64
	readyAt       time.Time
	users         map[string]*helperUser
}

type helperUser struct {
	Secret             string
	UserAdTag          string
	MaxTCPConns        int
	ExpirationRFC3339  string
	DataQuotaBytes     uint64
	MaxUniqueIPs       int
	CurrentConnections uint64
	ActiveIPs          []string
	Uplink             int64
	Downlink           int64
}

type helperConfig struct {
	APIListen     string
	MetricsListen string
	AuthHeader    string
	Users         map[string]*helperUser
}

func runHelperProcess() int {
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "missing helper subcommand")
		return 2
	}

	switch args[0] {
	case "--version":
		fmt.Println("telemt 1.2.3-test")
		return 0
	case "reload":
		return runHelperReload(args[1:])
	case "run":
		return runHelperForeground(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unsupported helper subcommand %q\n", args[0])
		return 2
	}
}

func runHelperReload(args []string) int {
	pidFile, _, err := parseHelperArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	if _, err := os.Stat(pidFile + ".reload_fail"); err == nil {
		fmt.Fprintln(os.Stderr, "reload failure requested")
		return 1
	}

	if err := os.WriteFile(pidFile+".reload", []byte("reload"), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	fmt.Println("helper reload requested")
	return 0
}

func runHelperForeground(args []string) int {
	pidFile, configPath, err := parseHelperArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	cfg, err := loadHelperConfig(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	state := &helperRuntime{
		configPath:  configPath,
		pidFilePath: pidFile,
		users:       make(map[string]*helperUser),
		revision:    1,
		readyAt:     time.Now().Add(helperHealthDelay()),
	}
	state.applyConfig(cfg)

	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.Remove(pidFile)

	apiServer := &http.Server{Addr: state.apiListen, Handler: helperAPIHandler(state)}
	metricsServer := &http.Server{Addr: state.metricsListen, Handler: helperMetricsHandler(state)}

	apiListener, err := net.Listen("tcp", state.apiListen)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer apiListener.Close()

	metricsListener, err := net.Listen("tcp", state.metricsListen)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer metricsListener.Close()

	fmt.Printf("helper started api=%s metrics=%s\n", state.apiListen, state.metricsListen)

	serverErrors := make(chan error, 2)
	go func() {
		serverErrors <- apiServer.Serve(apiListener)
	}()
	go func() {
		serverErrors <- metricsServer.Serve(metricsListener)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go state.watchReloadRequests(ctx)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case <-signals:
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = apiServer.Shutdown(shutdownCtx)
	_ = metricsServer.Shutdown(shutdownCtx)
	fmt.Println("helper shutdown complete")
	return 0
}

func helperHealthDelay() time.Duration {
	raw := strings.TrimSpace(os.Getenv("MTPROTO_HELPER_HEALTH_DELAY_MS"))
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0
	}
	return time.Duration(value) * time.Millisecond
}

func parseHelperArgs(args []string) (string, string, error) {
	pidFile := ""
	configPath := ""

	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--pid-file":
			index++
			if index >= len(args) {
				return "", "", fmt.Errorf("missing value for --pid-file")
			}
			pidFile = args[index]
		default:
			if !strings.HasPrefix(args[index], "-") {
				configPath = args[index]
			}
		}
	}

	if pidFile == "" {
		return "", "", fmt.Errorf("missing pid file")
	}
	return pidFile, configPath, nil
}

func helperAPIHandler(state *helperRuntime) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !state.authorized(r) {
			writeHelperError(w, http.StatusUnauthorized, "unauthorized", "Missing or invalid Authorization header")
			return
		}

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/health":
			if time.Now().Before(state.readyAt) {
				writeHelperError(w, http.StatusServiceUnavailable, "starting", "helper is still starting")
				return
			}
			writeHelperSuccess(w, telemtHealth{Status: "ok", ReadOnly: false}, state.currentRevision())
		case r.Method == http.MethodPost && r.URL.Path == "/v1/users":
			state.handleCreateUser(w, r)
		case strings.HasPrefix(r.URL.Path, "/v1/users/"):
			state.handleUserRoute(w, r)
		default:
			writeHelperError(w, http.StatusNotFound, "not_found", "Route not found")
		}
	})
}

func helperMetricsHandler(state *helperRuntime) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		state.mu.RLock()
		defer state.mu.RUnlock()

		var builder strings.Builder
		for username, user := range state.users {
			fmt.Fprintf(&builder, "telemt_user_octets_from_client{user=%q} %d\n", username, user.Uplink)
			fmt.Fprintf(&builder, "telemt_user_octets_to_client{user=%q} %d\n", username, user.Downlink)
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(builder.String()))
	})
}

func (state *helperRuntime) authorized(request *http.Request) bool {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return request.Header.Get("Authorization") == state.authHeader
}

func (state *helperRuntime) currentRevision() string {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return strconv.FormatUint(state.revision, 10)
}

func (state *helperRuntime) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	var request telemtCreateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeHelperError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if _, exists := state.users[request.Username]; exists {
		writeHelperError(w, http.StatusConflict, "user_exists", "User already exists")
		return
	}

	user := helperUserFromCreateRequest(&request)
	state.users[request.Username] = user
	state.revision++
	writeHelperSuccess(w, state.buildUserInfoLocked(request.Username, user), strconv.FormatUint(state.revision, 10))
}

func (state *helperRuntime) handleUserRoute(w http.ResponseWriter, r *http.Request) {
	username := strings.TrimPrefix(r.URL.Path, "/v1/users/")
	if username == "" || strings.Contains(username, "/") {
		writeHelperError(w, http.StatusNotFound, "not_found", "User not found")
		return
	}

	switch r.Method {
	case http.MethodGet:
		state.mu.RLock()
		user, exists := state.users[username]
		if !exists {
			state.mu.RUnlock()
			writeHelperError(w, http.StatusNotFound, "not_found", "User not found")
			return
		}
		info := state.buildUserInfoLocked(username, user)
		revision := strconv.FormatUint(state.revision, 10)
		state.mu.RUnlock()
		writeHelperSuccess(w, info, revision)
	case http.MethodPatch:
		var request telemtPatchUserRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeHelperError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}

		state.mu.Lock()
		user, exists := state.users[username]
		if !exists {
			state.mu.Unlock()
			writeHelperError(w, http.StatusNotFound, "not_found", "User not found")
			return
		}
		applyPatchToHelperUser(user, &request)
		state.revision++
		info := state.buildUserInfoLocked(username, user)
		revision := strconv.FormatUint(state.revision, 10)
		state.mu.Unlock()
		writeHelperSuccess(w, info, revision)
	case http.MethodDelete:
		state.mu.Lock()
		if _, exists := state.users[username]; !exists {
			state.mu.Unlock()
			writeHelperError(w, http.StatusNotFound, "not_found", "User not found")
			return
		}
		if len(state.users) <= 1 {
			state.mu.Unlock()
			writeHelperError(w, http.StatusConflict, "last_user_forbidden", "Cannot delete the last configured user")
			return
		}
		delete(state.users, username)
		state.revision++
		revision := strconv.FormatUint(state.revision, 10)
		state.mu.Unlock()
		writeHelperSuccess(w, map[string]any{"username": username}, revision)
	default:
		writeHelperError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Unsupported method")
	}
}

func helperUserFromCreateRequest(request *telemtCreateUserRequest) *helperUser {
	user := &helperUser{
		Secret:            request.Secret,
		UserAdTag:         request.UserAdTag,
		ExpirationRFC3339: request.ExpirationRFC3339,
	}
	if request.MaxTCPConns != nil {
		user.MaxTCPConns = *request.MaxTCPConns
	}
	if request.DataQuotaBytes != nil {
		user.DataQuotaBytes = *request.DataQuotaBytes
	}
	if request.MaxUniqueIPs != nil {
		user.MaxUniqueIPs = *request.MaxUniqueIPs
	}
	if strings.Contains(request.Username, "online") {
		user.CurrentConnections = 1
		user.ActiveIPs = []string{"198.51.100.10"}
	}
	return user
}

func applyPatchToHelperUser(user *helperUser, request *telemtPatchUserRequest) {
	if request.Secret != "" {
		user.Secret = request.Secret
	}
	if request.UserAdTag != "" {
		user.UserAdTag = request.UserAdTag
	}
	if request.MaxTCPConns != nil {
		user.MaxTCPConns = *request.MaxTCPConns
	}
	if request.ExpirationRFC3339 != "" {
		user.ExpirationRFC3339 = request.ExpirationRFC3339
	}
	if request.DataQuotaBytes != nil {
		user.DataQuotaBytes = *request.DataQuotaBytes
	}
	if request.MaxUniqueIPs != nil {
		user.MaxUniqueIPs = *request.MaxUniqueIPs
	}
}

func (state *helperRuntime) buildUserInfoLocked(username string, user *helperUser) *telemtUserInfo {
	return &telemtUserInfo{
		Username:            username,
		InRuntime:           true,
		UserAdTag:           user.UserAdTag,
		MaxTCPConns:         user.MaxTCPConns,
		ExpirationRFC3339:   user.ExpirationRFC3339,
		DataQuotaBytes:      user.DataQuotaBytes,
		MaxUniqueIPs:        user.MaxUniqueIPs,
		CurrentConnections:  user.CurrentConnections,
		ActiveUniqueIPs:     len(user.ActiveIPs),
		ActiveUniqueIPsList: append([]string(nil), user.ActiveIPs...),
		RecentUniqueIPs:     len(user.ActiveIPs),
		RecentUniqueIPsList: append([]string(nil), user.ActiveIPs...),
		TotalOctets:         uint64(user.Uplink + user.Downlink),
	}
}

func writeHelperSuccess(w http.ResponseWriter, data any, revision string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":       true,
		"data":     data,
		"revision": revision,
	})
}

func writeHelperError(w http.ResponseWriter, statusCode int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok": false,
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
		"request_id": 1,
	})
}

func (state *helperRuntime) watchReloadRequests(ctx context.Context) {
	reloadPath := state.pidFilePath + ".reload"
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := os.Stat(reloadPath); err != nil {
				continue
			}
			_ = os.Remove(reloadPath)

			cfg, err := loadHelperConfig(state.configPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "helper reload parse failed: %v\n", err)
				continue
			}

			state.applyConfig(cfg)
			fmt.Println("helper reload applied")
		}
	}
}

func (state *helperRuntime) applyConfig(cfg *helperConfig) {
	state.mu.Lock()
	defer state.mu.Unlock()

	state.apiListen = cfg.APIListen
	state.metricsListen = cfg.MetricsListen
	state.authHeader = cfg.AuthHeader

	previousUsers := state.users
	state.users = make(map[string]*helperUser, len(cfg.Users))
	for username, user := range cfg.Users {
		current := user
		if existing, ok := previousUsers[username]; ok {
			current.Uplink = existing.Uplink
			current.Downlink = existing.Downlink
			current.CurrentConnections = existing.CurrentConnections
			current.ActiveIPs = append([]string(nil), existing.ActiveIPs...)
		}
		if strings.Contains(username, "online") && current.CurrentConnections == 0 {
			current.CurrentConnections = 1
			current.ActiveIPs = []string{"198.51.100.10"}
		}
		state.users[username] = current
	}
	state.revision++
}

func loadHelperConfig(path string) (*helperConfig, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := &helperConfig{
		Users: make(map[string]*helperUser),
	}

	currentSection := ""
	lines := strings.Split(string(content), "\n")
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			currentSection = strings.TrimSpace(strings.Trim(line, "[]"))
			continue
		}

		key, value, ok := parseHelperAssignment(line)
		if !ok {
			continue
		}

		switch currentSection {
		case "server":
			if key == "metrics_listen" {
				cfg.MetricsListen, err = strconv.Unquote(value)
				if err != nil {
					return nil, err
				}
			}
		case "server.api":
			switch key {
			case "listen":
				cfg.APIListen, err = strconv.Unquote(value)
			case "auth_header":
				cfg.AuthHeader, err = strconv.Unquote(value)
			}
			if err != nil {
				return nil, err
			}
		case "access.users":
			username, err := decodeHelperKey(key)
			if err != nil {
				return nil, err
			}
			secret, err := strconv.Unquote(value)
			if err != nil {
				return nil, err
			}
			user := ensureHelperUser(cfg.Users, username)
			user.Secret = secret
		case "access.user_ad_tags":
			username, err := decodeHelperKey(key)
			if err != nil {
				return nil, err
			}
			adTag, err := strconv.Unquote(value)
			if err != nil {
				return nil, err
			}
			ensureHelperUser(cfg.Users, username).UserAdTag = adTag
		case "access.user_max_tcp_conns":
			username, err := decodeHelperKey(key)
			if err != nil {
				return nil, err
			}
			limit, err := strconv.Atoi(value)
			if err != nil {
				return nil, err
			}
			ensureHelperUser(cfg.Users, username).MaxTCPConns = limit
		case "access.user_expirations":
			username, err := decodeHelperKey(key)
			if err != nil {
				return nil, err
			}
			ensureHelperUser(cfg.Users, username).ExpirationRFC3339 = value
		case "access.user_data_quota":
			username, err := decodeHelperKey(key)
			if err != nil {
				return nil, err
			}
			quota, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return nil, err
			}
			ensureHelperUser(cfg.Users, username).DataQuotaBytes = quota
		case "access.user_max_unique_ips":
			username, err := decodeHelperKey(key)
			if err != nil {
				return nil, err
			}
			limit, err := strconv.Atoi(value)
			if err != nil {
				return nil, err
			}
			ensureHelperUser(cfg.Users, username).MaxUniqueIPs = limit
		}
	}

	if cfg.APIListen == "" || cfg.MetricsListen == "" {
		return nil, fmt.Errorf("helper config missing server.api.listen or server.metrics_listen")
	}
	return cfg, nil
}

func ensureHelperUser(users map[string]*helperUser, username string) *helperUser {
	user, exists := users[username]
	if !exists {
		user = &helperUser{}
		users[username] = user
	}
	return user
}

func parseHelperAssignment(line string) (string, string, bool) {
	key, value, found := strings.Cut(line, "=")
	if !found {
		return "", "", false
	}
	return strings.TrimSpace(key), strings.TrimSpace(value), true
}

func decodeHelperKey(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "\"") {
		return strconv.Unquote(raw)
	}
	return raw, nil
}
