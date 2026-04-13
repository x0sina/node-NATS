package mtproto

import (
	"strings"
	"testing"

	"github.com/pasarguard/node/common"
)

func TestNewConfigDefaultsInboundTagAndRejectsUserAccessMaps(t *testing.T) {
	t.Run("defaults inbound tag", func(t *testing.T) {
		cfg, err := NewConfig(`{"server":{"port":443},"censorship":{"tls_domain":"example.com"}}`)
		if err != nil {
			t.Fatalf("NewConfig returned error: %v", err)
		}
		if cfg.InboundTag != defaultInboundTag {
			t.Fatalf("expected default inbound tag %q, got %q", defaultInboundTag, cfg.InboundTag)
		}
	})

	t.Run("rejects access users", func(t *testing.T) {
		_, err := NewConfig(`{"access":{"users":{"alice":"00112233445566778899aabbccddeeff"}}}`)
		if err == nil {
			t.Fatal("expected config validation error")
		}
		if !strings.Contains(err.Error(), "access.users") {
			t.Fatalf("expected access.users validation error, got %v", err)
		}
	})
}

func TestConfigRenderIsDeterministicAndEnforcesLoopbackOverrides(t *testing.T) {
	cfg, err := NewConfig(`{
		"general": {"use_middle_proxy": true},
		"server": {
			"port": 443,
			"api": {"enabled": false, "listen": "0.0.0.0:9000"},
			"metrics_listen": "0.0.0.0:9001"
		},
		"censorship": {"tls_domain": "example.com"}
	}`)
	if err != nil {
		t.Fatalf("NewConfig returned error: %v", err)
	}

	users := map[string]*runtimeUser{
		"100.alice": {
			Username:          "100.alice",
			Secret:            "00112233445566778899aabbccddeeff",
			UserAdTag:         "ffeeddccbbaa99887766554433221100",
			MaxTCPConns:       4,
			ExpirationRFC3339: "2026-04-13T10:11:12Z",
			DataQuotaBytes:    4096,
			MaxUniqueIPs:      3,
		},
	}

	firstRender, err := cfg.Render(users, nil, 19091, 19092, "Bearer internal")
	if err != nil {
		t.Fatalf("Render returned error: %v", err)
	}
	secondRender, err := cfg.Render(users, nil, 19091, 19092, "Bearer internal")
	if err != nil {
		t.Fatalf("Render returned error: %v", err)
	}

	if firstRender != secondRender {
		t.Fatalf("expected deterministic render output")
	}

	requiredFragments := []string{
		`metrics_listen = "127.0.0.1:19092"`,
		`metrics_whitelist = ["127.0.0.1/32", "::1/128"]`,
		`enabled = true`,
		`listen = "127.0.0.1:19091"`,
		`auth_header = "Bearer internal"`,
		`whitelist = ["127.0.0.1/32", "::1/128"]`,
		`"100.alice" = "00112233445566778899aabbccddeeff"`,
		`"100.alice" = "ffeeddccbbaa99887766554433221100"`,
		`"100.alice" = 4`,
		`"100.alice" = 2026-04-13T10:11:12Z`,
		`"100.alice" = 4096`,
		`"100.alice" = 3`,
	}

	for _, fragment := range requiredFragments {
		if !strings.Contains(firstRender, fragment) {
			t.Fatalf("rendered config missing fragment %q\n%s", fragment, firstRender)
		}
	}
}

func TestActiveUsersFromCommonValidatesMTProtoFields(t *testing.T) {
	validUsers, err := ActiveUsersFromCommon([]*common.User{
		{
			Email:    "100.alice",
			Inbounds: []string{"mtproto-main"},
			Proxies: &common.Proxy{
				Mtproto: &common.Mtproto{
					Secret:            "00112233445566778899aabbccddeeff",
					UserAdTag:         "ffeeddccbbaa99887766554433221100",
					MaxTcpConns:       2,
					ExpirationRfc3339: "2026-04-13T10:11:12Z",
					DataQuotaBytes:    8192,
					MaxUniqueIps:      5,
				},
			},
		},
	}, "mtproto-main")
	if err != nil {
		t.Fatalf("ActiveUsersFromCommon returned error: %v", err)
	}

	user := validUsers["100.alice"]
	if user == nil {
		t.Fatalf("expected active user to be collected")
	}
	if user.MaxTCPConns != 2 || user.MaxUniqueIPs != 5 || user.DataQuotaBytes != 8192 {
		t.Fatalf("unexpected parsed user: %#v", user)
	}

	_, err = ActiveUsersFromCommon([]*common.User{
		{
			Email:    "100.alice",
			Inbounds: []string{"mtproto-main"},
			Proxies: &common.Proxy{
				Mtproto: &common.Mtproto{
					Secret: "not-hex",
				},
			},
		},
	}, "mtproto-main")
	if err == nil {
		t.Fatal("expected invalid secret validation error")
	}
	if !strings.Contains(err.Error(), "invalid secret") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}
