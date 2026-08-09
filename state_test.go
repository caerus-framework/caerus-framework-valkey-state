package cf_valkey_state

import (
	"context"
	"strings"
	"testing"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	cf_valkey "github.com/caerus-framework/caerus-framework-valkey"
)

func TestComponentContract(t *testing.T) {
	s := New()
	if s.Name() != ComponentName {
		t.Fatalf("Name() = %q, want %q", s.Name(), ComponentName)
	}
	if s.GetInitOrderStage() != ComponentStage {
		t.Fatalf("GetInitOrderStage() = %q, want %q", s.GetInitOrderStage(), ComponentStage)
	}
	var _ cf.CaerusComponent = s

	if c := s.Client(); c != nil {
		t.Fatal("Client() should be nil before Init")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Init: %v", err)
	}
}

func TestHealthAndMetricsBeforeInit(t *testing.T) {
	s := New()
	if err := s.Health(context.Background()); err == nil {
		t.Fatal("Health before Init should fail")
	}
	if ms := s.Metrics(); ms != nil {
		t.Fatalf("Metrics before Init = %+v, want nil", ms)
	}
	var _ cf.HealthProvider = s
	var _ cf_observability.MetricsProvider = s
}

func TestNewDefaults(t *testing.T) {
	s := New()
	if s.sessionTTL != defaultSessionTTL {
		t.Fatalf("default sessionTTL = %v, want %v", s.sessionTTL, defaultSessionTTL)
	}
	if s.cacheTTL != defaultCacheTTL {
		t.Fatalf("default cacheTTL = %v, want %v", s.cacheTTL, defaultCacheTTL)
	}
	if s.valkeyName != "" {
		t.Fatalf("default valkeyName = %q, want empty", s.valkeyName)
	}
}

func TestNewWithName(t *testing.T) {
	s := New(WithName("sessions"))
	if s.Name() != "sessions" {
		t.Fatalf("Name() = %q, want sessions", s.Name())
	}
}

func TestWithConfigOverridesOptions(t *testing.T) {
	s := New(
		WithSessionTTL(2*time.Hour),
		WithCacheTTL(10*time.Minute),
		WithConfig(StateConfig{SessionTTLSec: 3600, CacheTTLSec: 300}),
	)
	if s.sessionTTL != time.Hour {
		t.Fatalf("sessionTTL = %v, want 1h", s.sessionTTL)
	}
	if s.cacheTTL != 5*time.Minute {
		t.Fatalf("cacheTTL = %v, want 5m", s.cacheTTL)
	}
}

func TestGetDependencies(t *testing.T) {
	s := New()
	deps := s.GetDependencies()
	if len(deps) != 2 {
		t.Fatalf("GetDependencies() = %v, want [valkey logs]", deps)
	}
	if deps[0] != cf_valkey.ComponentName || deps[1] != "logs" {
		t.Fatalf("GetDependencies() = %v, want [valkey logs]", deps)
	}
	var _ cf.Dependencies = s

	named := New(WithValkeyName("cache"))
	deps = named.GetDependencies()
	if len(deps) != 2 || deps[0] != "cache" {
		t.Fatalf("GetDependencies() with named peer = %v, want [cache logs]", deps)
	}

	withSrc := New(WithConfigSource("state", "config/state.json"))
	deps = withSrc.GetDependencies()
	if len(deps) != 3 {
		t.Fatalf("GetDependencies() with source = %v, want [valkey logs configuration]", deps)
	}
	if deps[2] != "configuration" {
		t.Fatalf("GetDependencies() with source = %v, want configuration last", deps)
	}
}

func TestInitRequiresValkey(t *testing.T) {
	s := New()
	err := s.Init(context.Background(), cf.New())
	if err == nil {
		t.Fatal("Init without a valkey component should fail")
	}
	if !strings.Contains(err.Error(), `valkey component "valkey" is not registered`) {
		t.Fatalf("Init error = %v, want a valkey-not-registered error", err)
	}
}

func TestInitWithNamedValkeyMissing(t *testing.T) {
	s := New(WithValkeyName("cache"))
	err := s.Init(context.Background(), cf.New())
	if err == nil {
		t.Fatal("Init with a missing named valkey should fail")
	}
	if !strings.Contains(err.Error(), `valkey component "cache" is not registered`) {
		t.Fatalf("Init error = %v, want a cache-not-registered error", err)
	}
}

func TestInitRequiresValkeyInitialized(t *testing.T) {
	fw := cf.New()
	if err := fw.AddComponent(cf_valkey.New()); err != nil {
		t.Fatalf("AddComponent: %v", err)
	}
	s := New()
	err := s.Init(context.Background(), fw)
	if err == nil {
		t.Fatal("Init against an uninitialized valkey should fail")
	}
	if !strings.Contains(err.Error(), "is not initialized") {
		t.Fatalf("Init error = %v, want a valkey-not-initialized error", err)
	}
}

func TestInitResolvesValkeyByName(t *testing.T) {
	fw := cf.New()
	if err := fw.AddComponent(cf_valkey.New(cf_valkey.WithName("cache"))); err != nil {
		t.Fatalf("AddComponent: %v", err)
	}
	s := New(WithValkeyName("cache"))
	err := s.Init(context.Background(), fw)
	if err == nil {
		t.Fatal("Init against an uninitialized named valkey should fail")
	}
	// Finding the "cache" peer proves the by-name resolution worked; the only
	// failure left is the uninitialized client.
	if !strings.Contains(err.Error(), `valkey component "cache" is not initialized`) {
		t.Fatalf("Init error = %v, want a cache-not-initialized error", err)
	}
}

func TestInitTwiceIsIdempotent(t *testing.T) {
	s := New()
	err := s.Init(context.Background(), cf.New())
	if err == nil {
		t.Fatal("Init should fail without valkey")
	}
	err2 := s.Init(context.Background(), cf.New())
	if err2 == nil {
		t.Fatal("second Init should also fail without valkey")
	}
}

func TestKeyHelpers(t *testing.T) {
	s := New()
	// Before Init the helpers fall back to a plain ":"-join of namespace+parts.
	if k := s.sessionKey("tok"); k != "session:tok" {
		t.Fatalf("sessionKey = %q, want session:tok", k)
	}
	if k := s.cacheKey("users", "42"); k != "cache:users:42" {
		t.Fatalf("cacheKey = %q, want cache:users:42", k)
	}
	if k := s.rlKey("login:1.2.3.4"); k != "rl:login:1.2.3.4" {
		t.Fatalf("rlKey = %q, want rl:login:1.2.3.4", k)
	}
	if k := s.Key("lock", "x"); k != "lock:x" {
		t.Fatalf("Key = %q, want lock:x", k)
	}
}

func TestNewSourceEnvPrefix(t *testing.T) {
	s := New(WithConfigSource("valkey-state", "config/state.json"))
	if s.srcEnvPrefix != "VALKEY_STATE_" {
		t.Fatalf("srcEnvPrefix = %q, want VALKEY_STATE_", s.srcEnvPrefix)
	}
	s2 := New(WithConfigSource("state", "", WithSourceEnvPrefix("")))
	if s2.srcEnvPrefix != "" {
		t.Fatalf("srcEnvPrefix = %q, want empty", s2.srcEnvPrefix)
	}
}
