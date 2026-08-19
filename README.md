# caerus-framework-valkey-state

[![CI](https://github.com/caerus-framework/caerus-framework-valkey-state/actions/workflows/ci.yml/badge.svg)](https://github.com/caerus-framework/caerus-framework-valkey-state/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/caerus-framework/caerus-framework-valkey-state/graph/badge.svg)](https://codecov.io/gh/caerus-framework/caerus-framework-valkey-state)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Caerus Framework Valkey State Component. Keyed state services — **sessions**
(by id plus an optional per-user SET index), **cache**, and **rate-limit
counters** — on top of
[caerus-framework-valkey](https://github.com/caerus-framework/caerus-framework-valkey).
Apps do not import valkey-go or invent `KEYS` scans. Framework-owned
lifecycle, configuration (file + env + flags), live tunable reload with
last-good, logging through `logs`, observability health/metrics.

This is **not** valkey-queues (VPQ / jobs). Queues and state are siblings
on the same fridge; neither owns the other. HTTP 429 / `KeyFunc` /
fail-open stay on
[caerus-framework-http-ratelimiter](https://github.com/caerus-framework/caerus-framework-http-ratelimiter).
This module stores the count.

The component is a stateless **consumer** of a `CFValkey` peer: it holds the
peer pointer (never a client snapshot) and uses live `Client()` and
prefix-aware `Key()`. Reconnects stay on the valkey component.

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
	Observability: &cf.ObservabilitySettings{Bind: ":9090", ConfigSource: "observability"},
	Components: []cf.CaerusComponent{
		cf_postgres.New(cf_postgres.WithConfigSource("postgresql", "config/postgresql.json")),
		cf_valkey.New(cf_valkey.WithConfigSource("valkey", "config/valkey.json")),
		cf_valkey_state.New(cf_valkey_state.WithConfigSource("valkey-state", "config/valkey-state.json")),
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
state := cf_valkey_state.New(cf_valkey_state.WithConfigSource("valkey-state", "config/valkey-state.json"))
fw.AddComponent(logs)
fw.AddComponent(valkey) // GetDependencies() -> [logs configuration]
fw.AddComponent(state)  // GetDependencies() -> [valkey logs configuration]
```

In both shapes the component is `cf.ConfigSourceRegistrar`-self-sufficient:
`WithConfigSource` registers the `Source[StateConfig]` with the configuration
component during argv absorption, so `main` never touches
`os.Getenv`/`ParseFlags`. Prefer the **same string** for the config source
name and the component `Name()` (`"valkey-state"`) so `GetDependencies` and
`--valkey-state` / `VALKEY_STATE_` line up. You may still choose a shorter
source name (e.g. `"state"`) — then remember `GetDependencies` must use
`ComponentName` (`"valkey-state"`), not the source nickname.

## Usage

The app resolves the component once at `Init` and calls accessors per use.
Keys go through the peer’s `Key()` (`session:*`, `session-bind:*`,
`session-user:*`, `cache:*`, `rl:*`) plus valkey `WithKeyPrefix`.

`CFState.Key(parts…)` is the same prefix helper for **other** patterns
(e.g. `patterns.NewMutex`). Rate-limit storage keys stay unexported
(`rlKey`). Pass the **logical** key into `Allow` / `Peek` / `Reset`
(`login:`+email), not `prod:rl:login:…`.

### Sessions

Opaque JSON at `session:<id>` with a TTL. The app chooses the id (opaque
token). Optional **user index** (no Redis `KEYS`):

| Call | What it does |
|---|---|
| `CreateSession(ctx, id, v, ttl)` | By id only. `ListSessionsForUser` will not see it. |
| `CreateSession(..., WithSessionUser(userID))` | Also `SADD` `session-user:<user>` and `session-bind:<id>` (user id for SREM; the JSON is not parsed). |
| `ListSessionsForUser(ctx, userID)` | `SMEMBERS` + `EXISTS`; expired payloads are `SREM`’d (ghosts). O(that user’s sessions). |
| `RevokeAllForUser(ctx, userID, keep…)` | Delete every indexed session except `keep` (usually the current id). |
| `RevokeSession` | `DEL` payload + bind and `SREM` when indexed. |
| `TouchSession` | Sliding TTL on payload; bind and user SET PTTL bumped when indexed. |

Empty session/user id is an error. Valkey/JSON errors **do not include**
the session or user id — log those in the app if you need them.

Do **not** use `Allow` / counters to list sessions. Do **not** `KEYS`.
This module does not set cookies or CSRF tokens.

```go
sess := map[string]any{"user_id": "u1", "roles": []string{"admin"}}
_ = st.CreateSession(ctx, "tok-abc", sess, 24*time.Hour, cf_valkey_state.WithSessionUser("u1"))
var got map[string]any
found, _ := st.GetSession(ctx, "tok-abc", &got) // found=false on miss/expiry
st.SessionExists(ctx, "tok-abc")
st.TouchSession(ctx, "tok-abc", 15*time.Minute)
ids, _ := st.ListSessionsForUser(ctx, "u1")
_ = st.RevokeAllForUser(ctx, "u1", "tok-abc") // revoke others, keep current
st.RevokeSession(ctx, "tok-abc")
```

### Cache

JSON cache-aside. `CacheGetOrLoad` runs `load` at most once per key per
in-flight wave **in this process** (`singleflight`). SET after load
re-resolves `Client()`. Other pods still stampede on a cold key.

```go
_ = st.CacheSet(ctx, someStruct, time.Minute, "catalog", "sku-1")
var cached someStruct
_ = st.CacheGet(ctx, &cached, "catalog", "sku-1") // miss: no-op, dst untouched

val, shared, _ := st.CacheGetOrLoad(ctx, time.Minute, func(ctx context.Context) ([]byte, error) {
	return json.Marshal(fetchFromDB(ctx))
}, "catalog", "sku-2")
```

### Rate-limit counters (the store, not HTTP)

Fixed-window Lua (`INCR` + `PEXPIRE` on first count) or the nested
`rate_limit` memory map. HTTP 429 / `KeyFunc` / fail-open live on
http-ratelimiter, which calls this machine. Login lockout from an auth
handler should go through that HTTP limiter, not a raw `Allow` here.

```go
res, err := st.Allow(ctx, "login:"+email, 5, time.Minute)
if err != nil {
	// store failed; FailOpen / FailClosed is the HTTP module
}
```

`Peek` / `Reset` use the same logical key. There is no public
`RateLimitKey`. Sliding window / token bucket are not shipped; this is
fixed-window until a named product needs more Lua **here**.

### Counter store settings (`rate_limit`)

State **chooses** Lua vs the sticky-note map from nested file settings.
The app does not pass “use memory on this call.” Only **counters** get a
map — sessions and cache always need a live Valkey client.

| Setting | Meaning |
|---|---|
| `force_memory` | Always use the map (GH App / laptop). |
| `use_memory_fallback` | Lua first; on nil client or Eval error, use the map. |
| `memory_map_full_policy` | `allow` or `deny` when the map hits its cap (required when memory is on). |

### Health (`/readyz`)

State does **not** PING. Valkey’s `Health` already PINGs. Two shapes:

| Wiring | After Init, `Health` |
|---|---|
| **Path A** — `WithoutValkeyPeer` + memory on (counters only) | **Ready.** No fridge. Sessions/cache still error. |
| **Path B** — valkey declared (normal app) | **Ready only if `Client()` is non-nil.** Soft-init / DegradedMode can finish Init with a nil client; `/readyz` stays **red** until the peer connects. |

DegradedMode answers “may Initialize finish?” `/readyz` answers “send
LB traffic?” Do not mix them.

### GH App / laptop counters without Valkey

No session store in-process. Construct state **without** a valkey
component; turn memory on. `main` does not add `cf_valkey`.

```go
cf_valkey_state.New(
	cf_valkey_state.WithoutValkeyPeer(),
	cf_valkey_state.WithForceMemory(true),
	cf_valkey_state.WithMemoryMapFullPolicy("deny"),
	cf_valkey_state.WithConfigSource("valkey-state", "config/valkey-state.json"),
)
```

Init fails if memory is off. Mixed app (sessions + counters, **one**
valkey) uses Path B Health, not this recipe.

## Options

| Option | Description |
| --- | --- |
| `WithConfig(StateConfig)` | static config snapshot; non-zero fields override option-set defaults |
| `WithConfigSource(name, path, …)` | bind a configuration source for Init + `OnConfigReload`; the module registers the `Source[StateConfig]` itself (declares `configuration` dep) |
| `WithSessionTTL(d)` | default session TTL when a call leaves ttl at zero (default `24h`) |
| `WithSessionUser` on `CreateSession` | index that session under a user SET (list / revoke-all) |
| `WithCacheTTL(d)` | default cache TTL when a call leaves ttl at zero (default `5m`) |
| `WithValkeyName(name)` | bind to a named valkey peer (default `"valkey"`) |
| `WithoutValkeyPeer()` | omit valkey (counter memory only; sessions/cache error) |
| `WithUseMemoryFallback(bool)` / `WithForceMemory(bool)` | counter map (see `rate_limit`) |
| `WithMemoryMapFullPolicy("allow"\|"deny")` | required when counter memory is on |
| `WithName(name)` | custom component name for multiple instances (default `"valkey-state"`) |
| `WithLogger(*slog.Logger)` | explicit logger override; defaults to the framework `logs` component's logger (re-delivered on `logs` `Reconfigure`), falling back to `slog.Default()` |

## Configuration

Load `StateConfig` through the configuration component (`WithConfigSource`).
With the recommended source name `"valkey-state"`, the default `EnvPrefix` is
`VALKEY_STATE_` (override with `WithSourceEnvPrefix` on the source options).

Configuration overlay order is **file → env → flags** (later wins). Two
different shapes matter:

| Field group | In JSON/YAML | Via env |
|---|---|---|
| Session / cache TTLs | Top-level `session_ttl_sec`, `cache_ttl_sec` | Yes — `VALKEY_STATE_SESSION_TTL_SEC`, `VALKEY_STATE_CACHE_TTL_SEC` |
| Counter store (`rate_limit`) | Nested object (see below) | **No** direct nested walk — use flat `VALKEY_STATE_RATE_LIMIT_*` aliases |

The configuration component walks **top-level** struct fields for env overlay.
It does **not** recurse into nested structs. Session and cache TTLs are
top-level on `StateConfig`, so env works normally. The counter store lives
under nested `rate_limit` in the file; for env-only or PaaS overrides, this
module declares **flat alias fields** on `StateConfig` that `applyConfig`
merges into `rate_limit` (same pattern as postgres DSN overlays).

### File example (Kubernetes / canonical)

Use nested JSON or YAML when the file is the source of truth:

```json
{
  "session_ttl_sec": 86400,
  "cache_ttl_sec": 300,
  "rate_limit": {
    "use_memory_fallback": true,
    "force_memory": false,
    "memory_max_entries": 10000,
    "memory_map_full_policy": "deny"
  }
}
```

Do **not** flatten `use_memory_fallback` onto the top level next to
`session_ttl_sec` — that object is the counter store only, not HTTP policy
and not session TTLs.

### Env example (local / PaaS / fileless source)

When you need env without a nested path, use the alias keys (prefix +
`env` tag on `StateConfig`):

```text
VALKEY_STATE_SESSION_TTL_SEC=86400
VALKEY_STATE_CACHE_TTL_SEC=300
VALKEY_STATE_RATE_LIMIT_USE_MEMORY_FALLBACK=true
VALKEY_STATE_RATE_LIMIT_FORCE_MEMORY=false
VALKEY_STATE_RATE_LIMIT_MEMORY_MAX_ENTRIES=10000
VALKEY_STATE_RATE_LIMIT_MEMORY_MAP_FULL_POLICY=deny
```

| Env key (default prefix) | Maps to |
|---|---|
| `VALKEY_STATE_SESSION_TTL_SEC` | `session_ttl_sec` |
| `VALKEY_STATE_CACHE_TTL_SEC` | `cache_ttl_sec` |
| `VALKEY_STATE_RATE_LIMIT_USE_MEMORY_FALLBACK` | `rate_limit.use_memory_fallback` |
| `VALKEY_STATE_RATE_LIMIT_FORCE_MEMORY` | `rate_limit.force_memory` |
| `VALKEY_STATE_RATE_LIMIT_MEMORY_MAX_ENTRIES` | `rate_limit.memory_max_entries` |
| `VALKEY_STATE_RATE_LIMIT_MEMORY_MAP_FULL_POLICY` | `rate_limit.memory_map_full_policy` |

Wrong vs right:

```text
Wrong: VALKEY_STATE_rate_limit.use_memory_fallback
       → no dotted env paths; overlay does not walk into rate_limit

Wrong: RATE_LIMIT_USE_MEMORY_FALLBACK
       → wrong prefix; default source prefix is VALKEY_STATE_

Right: VALKEY_STATE_RATE_LIMIT_USE_MEMORY_FALLBACK=true
       → flat alias on StateConfig, merged in applyConfig

Right: config/valkey-state.json with nested "rate_limit": { ... }
       → file decode sets the nested struct directly
```

Reload: `OnConfigReload` re-reads the source and applies TTLs and counter
settings (last-good on validation failure). The valkey peer owns connection
rotation — a config reload here never rebuilds the Valkey client.

`Metrics` emits the following while initialized, nil before Init/after
Shutdown:

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
user session index, singleflight coalescing, Lua atomicity) run only when the gate env var is set:

```bash
docker run -d --rm -p 6379:6379 --name v valkey/valkey:8
VALKEY_ADDR=127.0.0.1:6379 go test -race ./...
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
