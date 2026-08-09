package cf_valkey_state

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_valkey "github.com/caerus-framework/caerus-framework-valkey"
	"github.com/valkey-io/valkey-go"
)

func addComponent(t *testing.T, fw *cf.CaerusFramework, c cf.CaerusComponent) {
	t.Helper()
	if err := fw.AddComponent(c); err != nil {
		t.Fatalf("AddComponent: %v", err)
	}
}

func newFramework(t *testing.T) *cf.CaerusFramework {
	t.Helper()
	fw := cf.New()
	addComponent(t, fw, cf_logs.New(cf_logs.WithWriter(io.Discard)))
	return fw
}

// setupState boots a framework with a real valkey (VALKEY_ADDR) and the state
// component, flushes the server, and returns the state plus a raw client for
// assertions.
func setupState(t *testing.T) (*CFState, valkey.Client) {
	t.Helper()
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}
	fw := newFramework(t)
	vk := cf_valkey.New(
		cf_valkey.WithAddress(addr),
		cf_valkey.WithKeyPrefix("valkey-state-test"),
	)
	addComponent(t, fw, vk)
	s := New()
	addComponent(t, fw, s)
	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })

	raw := vk.Client()
	if err := raw.Do(context.Background(), raw.B().Flushdb().Build()).Error(); err != nil {
		t.Fatalf("Flushdb: %v", err)
	}
	return s, raw
}

func pttl(t *testing.T, raw valkey.Client, key string) time.Duration {
	t.Helper()
	resp := raw.Do(context.Background(), raw.B().Pttl().Key(key).Build())
	if resp.Error() != nil {
		t.Fatalf("Pttl: %v", resp.Error())
	}
	ms, err := resp.AsInt64()
	if err != nil {
		t.Fatalf("Pttl AsInt64: %v", err)
	}
	return time.Duration(ms) * time.Millisecond
}

type testSession struct {
	UserID   string `json:"user_id"`
	Verified bool   `json:"verified"`
}

func TestIntegrationSessionLifecycle(t *testing.T) {
	s, _ := setupState(t)
	ctx := context.Background()

	if err := s.CreateSession(ctx, "s1", testSession{UserID: "u1", Verified: true}, 10*time.Second); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	var got testSession
	found, err := s.GetSession(ctx, "s1", &got)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if !found {
		t.Fatal("GetSession should find s1")
	}
	if got.UserID != "u1" || !got.Verified {
		t.Fatalf("session = %+v, want {u1 true}", got)
	}
	exists, err := s.SessionExists(ctx, "s1")
	if err != nil {
		t.Fatalf("SessionExists: %v", err)
	}
	if !exists {
		t.Fatal("SessionExists should report s1 present")
	}

	if err := s.RevokeSession(ctx, "s1"); err != nil {
		t.Fatalf("RevokeSession: %v", err)
	}
	found, err = s.GetSession(ctx, "s1", &got)
	if err != nil {
		t.Fatalf("GetSession after revoke: %v", err)
	}
	if found {
		t.Fatal("GetSession should miss after revoke")
	}
}

func TestIntegrationSessionTouch(t *testing.T) {
	s, raw := setupState(t)
	ctx := context.Background()

	if err := s.CreateSession(ctx, "s2", testSession{UserID: "u2"}, 10*time.Second); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if d := pttl(t, raw, rawKey(s, "session", "s2")); d < 8*time.Second || d > 10*time.Second {
		t.Fatalf("initial PTTL = %v, want ~10s", d)
	}
	if err := s.TouchSession(ctx, "s2", 30*time.Second); err != nil {
		t.Fatalf("TouchSession: %v", err)
	}
	if d := pttl(t, raw, rawKey(s, "session", "s2")); d < 25*time.Second || d > 30*time.Second {
		t.Fatalf("touched PTTL = %v, want ~30s", d)
	}
}

func TestIntegrationSessionExpiry(t *testing.T) {
	s, _ := setupState(t)
	ctx := context.Background()

	if err := s.CreateSession(ctx, "s3", testSession{UserID: "u3"}, 300*time.Millisecond); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	time.Sleep(600 * time.Millisecond)
	var got testSession
	found, err := s.GetSession(ctx, "s3", &got)
	if err != nil {
		t.Fatalf("GetSession after expiry: %v", err)
	}
	if found {
		t.Fatal("GetSession should miss after TTL expiry")
	}
}

func TestIntegrationCacheGetSet(t *testing.T) {
	s, _ := setupState(t)
	ctx := context.Background()

	var got map[string]int
	if err := s.CacheGet(ctx, &got, "config", "nested"); err != nil {
		t.Fatalf("CacheGet on empty key: %v", err)
	}
	if got != nil {
		t.Fatalf("CacheGet on miss = %v, want nil", got)
	}

	if err := s.CacheSet(ctx, map[string]int{"a": 1}, time.Minute, "config", "nested"); err != nil {
		t.Fatalf("CacheSet: %v", err)
	}
	got = nil
	if err := s.CacheGet(ctx, &got, "config", "nested"); err != nil {
		t.Fatalf("CacheGet after set: %v", err)
	}
	if got["a"] != 1 {
		t.Fatalf("CacheGet = %v, want map[a:1]", got)
	}
}

func TestIntegrationCacheGetOrLoad(t *testing.T) {
	s, _ := setupState(t)
	ctx := context.Background()

	var loads atomic.Int64
	load := func(ctx context.Context) ([]byte, error) {
		loads.Add(1)
		return []byte("v1"), nil
	}
	val, shared, err := s.CacheGetOrLoad(ctx, time.Minute, load, "users", "42")
	if err != nil {
		t.Fatalf("CacheGetOrLoad: %v", err)
	}
	if string(val) != "v1" || shared {
		t.Fatalf("first = %q shared=%v, want v1 false", val, shared)
	}
	if loads.Load() != 1 {
		t.Fatalf("loads after first = %d, want 1", loads.Load())
	}

	// Concurrent callers on a cold key coalesce onto one in-flight wave. The
	// load sleeps to widen the flight window; singleflight is process-local, so
	// a caller that only reaches the group after the flight finishes may start
	// a second one. Assert the stampede is bounded (way below the 20 callers),
	// not that it is exactly one.
	loads.Store(0)
	slow := func(ctx context.Context) ([]byte, error) {
		loads.Add(1)
		time.Sleep(100 * time.Millisecond)
		return []byte("v1"), nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _, err := s.CacheGetOrLoad(ctx, time.Minute, slow, "users", "43")
			if err != nil {
				t.Errorf("concurrent CacheGetOrLoad: %v", err)
				return
			}
			if string(v) != "v1" {
				t.Errorf("concurrent value = %q, want v1", v)
			}
		}()
	}
	wg.Wait()
	if n := loads.Load(); n < 1 || n > 2 {
		t.Fatalf("loads under concurrency = %d, want 1 or 2 (bounded stampede)", n)
	}
	after := loads.Load()

	// A warm key never calls load again.
	val, _, err = s.CacheGetOrLoad(ctx, time.Minute, load, "users", "42")
	if err != nil {
		t.Fatalf("CacheGetOrLoad warm: %v", err)
	}
	if string(val) != "v1" {
		t.Fatalf("warm value = %q, want v1", val)
	}
	if loads.Load() != after {
		t.Fatalf("loads after warm read = %d, want %d (unchanged)", loads.Load(), after)
	}
}

func TestIntegrationRateLimit(t *testing.T) {
	s, _ := setupState(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		res, err := s.RateLimit(ctx, "login:user@example.com", 3, 60*time.Second)
		if err != nil {
			t.Fatalf("RateLimit: %v", err)
		}
		if !res.Allowed {
			t.Fatalf("call %d should be allowed", i+1)
		}
		if res.Count != int64(i+1) {
			t.Fatalf("count = %d, want %d", res.Count, i+1)
		}
		if res.ResetIn <= 0 || res.ResetIn > 60*time.Second {
			t.Fatalf("ResetIn = %v, want within window", res.ResetIn)
		}
	}
	res, err := s.RateLimit(ctx, "login:user@example.com", 3, 60*time.Second)
	if err != nil {
		t.Fatalf("RateLimit (over): %v", err)
	}
	if res.Allowed {
		t.Fatal("4th call should be rejected")
	}
	if res.Count != 4 {
		t.Fatalf("count = %d, want 4", res.Count)
	}
}

func TestIntegrationRateLimitWindowReset(t *testing.T) {
	s, _ := setupState(t)
	ctx := context.Background()

	res, err := s.RateLimit(ctx, "ip:1.2.3.4", 1, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("RateLimit: %v", err)
	}
	if !res.Allowed {
		t.Fatal("first call should be allowed")
	}
	time.Sleep(350 * time.Millisecond)
	res, err = s.RateLimit(ctx, "ip:1.2.3.4", 1, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("RateLimit after reset: %v", err)
	}
	if !res.Allowed || res.Count != 1 {
		t.Fatalf("after window reset = %+v, want allowed count=1", res)
	}
}

func TestIntegrationRateLimitAtomicity(t *testing.T) {
	s, _ := setupState(t)
	ctx := context.Background()

	const n = 25
	var wg sync.WaitGroup
	var allowed atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.RateLimit(ctx, "burst", 100, time.Minute)
			if err != nil {
				t.Errorf("RateLimit: %v", err)
				return
			}
			if res.Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != n {
		t.Fatalf("allowed = %d, want %d (every concurrent call under limit)", allowed.Load(), n)
	}
	res, err := s.RateLimit(ctx, "burst", 100, time.Minute)
	if err != nil {
		t.Fatalf("final RateLimit: %v", err)
	}
	if res.Count != n+1 {
		t.Fatalf("final count = %d, want %d", res.Count, n+1)
	}
}

func TestIntegrationNamedValkeyPeer(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}
	fw := newFramework(t)
	vk := cf_valkey.New(cf_valkey.WithAddress(addr), cf_valkey.WithName("cache"))
	addComponent(t, fw, vk)
	s := New(WithValkeyName("cache"))
	addComponent(t, fw, s)
	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })

	if err := s.CreateSession(context.Background(), "named1", testSession{UserID: "u"}, time.Minute); err != nil {
		t.Fatalf("CreateSession via named peer: %v", err)
	}
}

func TestIntegrationHealthAndMetrics(t *testing.T) {
	s, _ := setupState(t)
	ctx := context.Background()

	if err := s.Health(ctx); err != nil {
		t.Fatalf("Health after init: %v", err)
	}
	if ms := s.Metrics(); ms == nil {
		t.Fatal("Metrics after init should be non-nil")
	}

	if err := s.CreateSession(ctx, "m1", testSession{UserID: "u"}, time.Minute); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	found := false
	for _, m := range s.Metrics() {
		if m.Name == "valkey_state_sessions_created_total" && m.Value == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("sessions_created counter should be 1 after one CreateSession")
	}
}

// rawKey resolves the exact valkey key for a state namespace through the
// component's prefix-aware Key helper.
func rawKey(s *CFState, parts ...string) string {
	return s.Key(parts...)
}

var _ = errors.Is
