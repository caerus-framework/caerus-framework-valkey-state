# caerus-framework-valkey-state

[![CI](https://github.com/caerus-framework/caerus-framework-valkey-state/actions/workflows/ci.yml/badge.svg)](https://github.com/caerus-framework/caerus-framework-valkey-state/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/caerus-framework/caerus-framework-valkey-state/graph/badge.svg)](https://codecov.io/gh/caerus-framework/caerus-framework-valkey-state)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Caerus Framework Valkey State Component. Keyed state services — **sessions**,
**cache**, and **rate limiting** — built on top of the
[caerus-framework-valkey](https://github.com/caerus-framework/caerus-framework-valkey)
component, so apps (e.g. `caerus-auth-api`) don't import valkey-go or manage
keys/TTLs themselves. Framework-owned lifecycle, configuration (file + env +
flags), live tunable reload with last-good semantics, logging through the
framework `logs` component, and observability health/metrics.

The component is a stateless **consumer** of a `CFValkey` peer: it holds the
peer component pointer (never a client snapshot) and builds every command
through the peer's live `Client()` and prefix-aware `Key()`, so reconnects and
key prefixes stay consistent and the peer's `OnConfigReload` owns client
rotation.

## Wiring

Two wiring shapes are supported. Prefer the **app-owned** shape (demoapp
golden path): `main` declares only the chassis (valkey-state alongside
postgres / valkey) and the app class; product machinery that uses sessions,
cache, or rate limits lives under the app and resolves the component as a peer
at `Init`. Use the simple `main`-level shape for one-off binaries.

### App-owned consumer (golden — demoapp pattern)

`main` declares valkey-state as chassis and runs the app class; it never
touches the component itself:

```go
fw := cf.New(&cf.FrameworkOptions{
	Logs: &cf.LogsSettings{Format: "json", Level: "info", ConfigSource: "logs"},
	Observability: &cf.ObservabilitySettings{Address: ":9090", ConfigSource: "observability"},
	Components: []cf.CaerusComponent{
		cf_postgres.New(cf_postgres.WithConfigSource("postgresql", "config/postgresql.json")),
		cf_valkey.New(cf_valkey.WithConfigSource("valkey", "config/valkey.json")),
		cf_valkey_state.New(cf_valkey_state.WithConfigSource("state", "config/state.json")),
		app.New(app.Options{}),
	},
})
if err := fw.RunWithSignals(context.Background()); err != nil {
	log.Fatal(err)
}
```

The app resolves the valkey-state **component pointer** once at `Init` (never a
client snapshot), declares it in `GetDependencies`, and calls the accessors per
use:

```go
type App struct {
	state *cf_valkey_state.CFState
}

func (a *App) GetDependencies() []string {
	return []string{cf_valkey_state.ComponentName} // + logs, chassis peers
}

func (a *App) Init(ctx context.Context, fw *cf.CaerusFramework) error {
	st, ok := cf.Get[*cf_valkey_state.CFState](fw)
	if !ok {
		return errors.New("app: valkey-state component missing")
	}
	a.state = st
	return nil
}
```

Multiple valkey-state instances in one process use `WithName` +
`GetByName[*cf_valkey_state.CFState](fw, "sessions")`; a state component bound
to a named valkey peer uses `WithValkeyName` (its `GetDependencies` reports the
peer's actual name, so framework `Validate` passes).

### Simple `main`-level wiring

For a one-off binary, register the components directly and use
`cf.MustGet` to reach the component:

```go
fw := cf.New()

logs := cf_logs.New(cf_logs.WithWriter(os.Stdout))
valkey := cf_valkey.New(cf_valkey.WithConfigSource("valkey", "config/valkey.json"))
state := cf_valkey_state.New(cf_valkey_state.WithConfigSource("state", "config/state.json"))
fw.AddComponent(logs)
fw.AddComponent(valkey) // GetDependencies() -> [logs configuration]
fw.AddComponent(state)  // GetDependencies() -> [valkey logs configuration]
```

In both shapes the component is `cf.ConfigSourceRegistrar`-self-sufficient:
`WithConfigSource` registers the `Source[StateConfig]` with the configuration
component during argv absorption, so `main` never touches
`os.Getenv`/`ParseFlags`. The `--state` path flag and per-field flags come from
the source declaration.

## Usage

The app resolves the component once at `Init` (see Wiring above) and calls the
accessors per use. All keys are namespaced (`session:*`, `cache:*`, `rl:*`) and
built through the peer's prefix-aware `Key()`, so the valkey `WithKeyPrefix`
applies on top.

```go
// sessions — opaque JSON values with a TTL
sess := map[string]any{"user_id": "u1", "roles": []string{"admin"}}
_ = st.CreateSession(ctx, "tok-abc", sess, 24*time.Hour) // ttl 0 → configured default
var got map[string]any
found, _ := st.GetSession(ctx, "tok-abc", &got)          // found=false on miss/expiry
st.SessionExists(ctx, "tok-abc")
st.TouchSession(ctx, "tok-abc", 15*time.Minute)          // sliding window
st.RevokeSession(ctx, "tok-abc")

// cache — JSON cache-aside with a singleflight get-or-load
_ = st.CacheSet(ctx, someStruct, time.Minute, "catalog", "sku-1")
var cached someStruct
_ = st.CacheGet(ctx, &cached, "catalog", "sku-1") // no-op miss (dst untouched)

val, shared, _ := st.CacheGetOrLoad(ctx, time.Minute, func(ctx context.Context) ([]byte, error) {
	return json.Marshal(fetchFromDB(ctx))
}, "catalog", "sku-2")
// concurrent callers coalesce onto one load (process-local singleflight)

// rate limiting — atomic fixed-window counter (Lua)
res, _ := st.RateLimit(ctx, "login:"+email, 5, time.Minute)
if !res.Allowed {
	// respond 429; res.ResetIn is the window time remaining (Retry-After)
}
// res.Count is the current count within the window
```

`CacheGetOrLoad`'s `load` runs at most once per key per in-flight wave inside
this process (same singleflight semantics as the valkey `patterns` package);
other pods still stampede on a cold key — compose with a valkey `patterns`
mutex for cross-pod coalescing. Rate-limit errors are returned to the caller:
fail-open/fail-closed policy belongs to the app.

## Options

| Option | Description |
| --- | --- |
| `WithConfig(StateConfig)` | static config snapshot; non-zero fields override option-set defaults |
| `WithConfigSource(name, path, …)` | bind a configuration source for Init + `OnConfigReload`; the module registers the `Source[StateConfig]` itself (declares `configuration` dep) |
| `WithSessionTTL(d)` | default session TTL when a call leaves ttl at zero (default `24h`) |
| `WithCacheTTL(d)` | default cache TTL when a call leaves ttl at zero (default `5m`) |
| `WithValkeyName(name)` | bind to a named valkey peer (`WithName` on the valkey side; default `"valkey"`) |
| `WithName(name)` | custom component name for multiple instances (default `"valkey-state"`) |
| `WithLogger(*slog.Logger)` | explicit logger override; defaults to the framework `logs` component's logger (re-delivered on `logs` `Reconfigure`), falling back to `slog.Default()` |

## Configuration

Load `StateConfig` through the configuration component. The default `EnvPrefix`
is `STATE_` (from the source name); `env` tags map `STATE_SESSION_TTL_SEC`,
`STATE_CACHE_TTL_SEC`.

```json
{
  "session_ttl_sec": 86400,
  "cache_ttl_sec": 300
}
```

The tunables apply live: on reload `OnConfigReload` re-reads the source and
replaces the TTL defaults (last-good on failure). The valkey peer owns
connection rotation, so a reload never rebuilds anything here.

`Health` reports initialized/uninitialized (the peer's client is the liveness
source; real connectivity is the valkey component's own `Health`, aggregated by
observability's `/readyz`). `Metrics` emits the following while initialized,
nil before Init/after Shutdown:

| Metric | Type | Labels |
| --- | --- | --- |
| `valkey_state_info` | gauge 1 | `component` |
| `valkey_state_config_reloads_total` | counter | `component` |
| `valkey_state_sessions_created_total` | counter | `component` |
| `valkey_state_sessions_revoked_total` | counter | `component` |
| `valkey_state_sessions_touched_total` | counter | `component` |
| `valkey_state_cache_hits_total` | counter | `component` |
| `valkey_state_cache_misses_total` | counter | `component` |
| `valkey_state_rate_allowed_total` | counter | `component` |
| `valkey_state_rate_rejected_total` | counter | `component` |

Counters appear at zero until first fired, so the series are always present
while initialized. Cache hit ratio is
`valkey_state_cache_hits_total / (valkey_state_cache_hits_total + valkey_state_cache_misses_total)`;
rate-limit rejection rate is `rate(valkey_state_rate_rejected_total[5m])`.

## Tests

Unit tests cover the component contract, options/config layering, dependency
declaration, and Init failure modes with no external service. Integration tests
(real session/cache/rate-limit behavior, TTL expiry, sliding-window touch,
singleflight coalescing, Lua atomicity) run only when the gate env var is set:

```bash
docker run -d --rm -p 6379:6379 --name v valkey/valkey:8
VALKEY_ADDR=127.0.0.1:6379 go test -race ./...
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
