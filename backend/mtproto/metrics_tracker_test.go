package mtproto

import (
	"strings"
	"testing"
)

func TestParseMetricsExtractsDirectionalUserCounters(t *testing.T) {
	payload := `
# HELP telemt_user_octets_from_client Bytes received from client
telemt_user_octets_from_client{user="100.alice"} 123
telemt_user_octets_to_client{user="100.alice"} 456
telemt_user_octets_from_client{user="200.bob"} 10
telemt_user_octets_to_client{user="200.bob"} 20
telemt_connections_total 99
`

	counters, err := parseMetrics(strings.NewReader(payload))
	if err != nil {
		t.Fatalf("parseMetrics returned error: %v", err)
	}

	if counters["100.alice"].Uplink != 123 || counters["100.alice"].Downlink != 456 {
		t.Fatalf("unexpected alice counters: %#v", counters["100.alice"])
	}
	if counters["200.bob"].Uplink != 10 || counters["200.bob"].Downlink != 20 {
		t.Fatalf("unexpected bob counters: %#v", counters["200.bob"])
	}
}

func TestTrafficTrackerSupportsResetDeletionAndAggregation(t *testing.T) {
	tracker := newTrafficTracker()

	tracker.Sync(map[string]userTrafficCounters{
		"100.alice": {Uplink: 100, Downlink: 200},
		"200.bob":   {Uplink: 10, Downlink: 20},
	}, map[string]struct{}{
		"100.alice": {},
		"200.bob":   {},
	})

	userStats := tracker.UserStats("100.alice", "mtproto", false)
	if len(userStats.GetStats()) != 2 {
		t.Fatalf("expected two directional stats, got %d", len(userStats.GetStats()))
	}

	aggregate := tracker.AggregateStats("mtproto", "mtproto", false)
	if len(aggregate.GetStats()) != 2 {
		t.Fatalf("expected aggregate uplink/downlink stats, got %d", len(aggregate.GetStats()))
	}

	tracker.Sync(map[string]userTrafficCounters{
		"100.alice": {Uplink: 150, Downlink: 250},
	}, map[string]struct{}{
		"100.alice": {},
	})

	usersStats := tracker.UsersStats("mtproto", true)
	if len(usersStats.GetStats()) != 4 {
		t.Fatalf("expected pending stats for alice and deleted bob before reset, got %d", len(usersStats.GetStats()))
	}

	afterReset := tracker.UsersStats("mtproto", false)
	if len(afterReset.GetStats()) != 0 {
		t.Fatalf("expected tracker to be empty after reset, got %d", len(afterReset.GetStats()))
	}
}
