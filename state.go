package cf_valkey_state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	cf_valkey "github.com/caerus-framework/caerus-framework-valkey"
	"github.com/valkey-io/valkey-go"
	"golang.org/x/sync/singleflight"
)

const (
	// ComponentName is the framework component name for the valkey-state
	// component. It is the identifier other components use in GetDependencies
	// to require it.
	ComponentName = "valkey-state"

	// ComponentStage is the stage data-layer components initialize in. It is
	// not a built-in bootstrap stage; AddComponent registers it automatically
	// the first time a component declares it.
	ComponentStage = cf.Stage("data")
)

// Key namespaces this component owns inside the valkey component's prefix
// space. All keys are built through the peer's Key, so the valkey key prefix
// (WithKeyPrefix) applies on top.
const (
	nsSession   = "session"
	nsCache     = "cache"
	nsRateLimit = "rl"
)

// Defaults applied when a call leaves the TTL at zero. Overridable per call
// and via StateConfig.
const (
	defaultSessionTTL = 24 * time.Hour
	defaultCacheTTL   = 5 * time.Minute
)

// luaRateLimit is an atomic fixed-window counter: INCR, set the expiry only on
// the first increment (so a key never leaks without a TTL), then return the
// count and the remaining time in the window.
const luaRateLimit = `local n = redis.call("INCR", KEYS[1])
if n == 1 then
  redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
return {n, redis.call("PTTL", KEYS[1])}`

// StateConfig is the file/env-drivable behavior configuration. Load it through
// the configuration component (caerus-framework-configuration) and pass it via
// WithConfigSource; both JSON and YAML tags are provided.
type StateConfig struct {
	// SessionTTLSec is the default session TTL in seconds when a call leaves
	// ttl at zero (default 24h).
	SessionTTLSec int64 `json:"session_ttl_sec,omitempty" yaml:"session_ttl_sec,omitempty" env:"SESSION_TTL_SEC"`
	// CacheTTLSec is the default cache TTL in seconds when a call leaves ttl
	// at zero (default 5m).
	CacheTTLSec int64 `json:"cache_ttl_sec,omitempty" yaml:"cache_ttl_sec,omitempty" env:"CACHE_TTL_SEC"`
}

// Option configures the state component at construction time.
type Option func(*options)

type options struct {
	loaded       *StateConfig // set by WithConfig; overrides option-set defaults
	configSource string       // named configuration source for live reload
	configPath   string       // source file path (module self-registration)
	srcEnvPrefix string       // source env overlay prefix (default: NAME_)
	srcFormat    cf_configuration.Format
	srcFormatSet bool
	sessionTTL   time.Duration
	cacheTTL     time.Duration
	valkeyName   string // valkey peer component name; empty means ComponentName
	logger       *slog.Logger
	loggerSet    bool   // true when WithLogger was called explicitly
	name         string // custom component name; empty means use ComponentName
}

// SourceOption configures the self-registered configuration source created by
// WithConfigSource.
type SourceOption func(*sourceOptions)

type sourceOptions struct {
	envPrefix string
	format    cf_configuration.Format
	formatSet bool
}

// WithSourceEnvPrefix sets the environment overlay prefix for the source
// (default: the uppercase source name with "-" replaced by "_", plus "_").
// An empty prefix disables env overlay.
func WithSourceEnvPrefix(prefix string) SourceOption {
	return func(o *sourceOptions) { o.envPrefix = prefix }
}

// WithSourceFormat forces the file format instead of inferring it from the
// path extension (".yaml"/".yml" → YAML; anything else JSON).
func WithSourceFormat(f cf_configuration.Format) SourceOption {
	return func(o *sourceOptions) { o.format = f; o.formatSet = true }
}

// defaultSourceEnvPrefix derives an environment prefix from a source name.
func defaultSourceEnvPrefix(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_"
}

// WithConfig sets a static configuration snapshot. Non-zero fields of cfg
// override the values set by the convenience options. Prefer WithConfigSource
// when using caerus-framework-configuration with hot-reload.
func WithConfig(cfg StateConfig) Option {
	return func(o *options) { o.loaded = &cfg }
}

// WithConfigSource binds this component to a named configuration source and
// registers that source with the configuration component (via the framework's
// ConfigSourceRegistrar pass during argv absorption). The module owns the
// Source: the config type, the default EnvPrefix and its Owner (Name(), so
// named instances reload correctly). main only points the instance at where
// the config lives.
//
//	cf_valkey_state.New(cf_valkey_state.WithConfigSource("state", "config/state.json"))
//	cf_valkey_state.New(cf_valkey_state.WithConfigSource("sess", "/etc/app/sess.yaml",
//	    cf_valkey_state.WithSourceFormat(cf_configuration.FormatYAML)))
//
// A path of "" registers an env-only (fileless) source when the EnvPrefix is
// non-empty. The path CLI override stays --<source-name> (ParseFlags).
// Declares a dependency on "configuration".
func WithConfigSource(name, path string, opts ...SourceOption) Option {
	return func(o *options) {
		so := sourceOptions{envPrefix: defaultSourceEnvPrefix(name)}
		for _, opt := range opts {
			opt(&so)
		}
		o.configSource = name
		o.configPath = path
		o.srcEnvPrefix = so.envPrefix
		o.srcFormat = so.format
		o.srcFormatSet = so.formatSet
	}
}

// WithSessionTTL sets the default session TTL used when a session call leaves
// ttl at zero (default 24h).
func WithSessionTTL(d time.Duration) Option {
	return func(o *options) { o.sessionTTL = d }
}

// WithCacheTTL sets the default cache TTL used when a cache call leaves ttl at
// zero (default 5m).
func WithCacheTTL(d time.Duration) Option {
	return func(o *options) { o.cacheTTL = d }
}

// WithValkeyName binds the state component to a valkey component with the
// given name (WithName on the valkey side). The default is the valkey
// ComponentName ("valkey").
func WithValkeyName(name string) Option {
	return func(o *options) { o.valkeyName = name }
}

// WithLogger overrides the logger used for component diagnostics. By default
// the component logs through the framework logs component (declared in
// GetDependencies); WithLogger is an explicit override for tests and embedded
// use and wins over the framework logger. slog.Default() remains the fallback
// only when neither is available.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) { o.logger = logger; o.loggerSet = true }
}

// WithName sets a custom component name, allowing multiple state instances in
// the same process. The default name is "valkey-state" (ComponentName).
// Retrieve named instances with GetByName[*CFState](fw, "sessions").
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// CFState is the caerus-framework-valkey-state component. It is a stateless
// consumer of a cf_valkey.CFValkey peer: it holds the peer component (never a
// client snapshot) and builds every command through the peer's live Client()
// and prefix-aware Key(), so reconnects and key prefixes stay consistent. It
// provides three keyed state services over that client:
//
//   - Sessions: opaque JSON values with a TTL and sliding-window touch.
//   - Cache: JSON cache-aside plus a singleflight get-or-load.
//   - Rate limit: atomic fixed-window counters.
type CFState struct {
	mu           sync.RWMutex
	configSource string
	configPath   string
	srcEnvPrefix string
	srcFormat    cf_configuration.Format
	srcFormatSet bool
	sessionTTL   time.Duration
	cacheTTL     time.Duration
	valkeyName   string
	loggerSet    bool
	vk           *cf_valkey.CFValkey // peer component, resolved at Init
	logger       *slog.Logger
	logsSub      *cf_logs.Subscription
	fw           *cf.CaerusFramework
	name         string // custom name; empty means use ComponentName
	sf           singleflight.Group

	sessionsCreated atomic.Uint64
	sessionsRevoked atomic.Uint64
	sessionsTouched atomic.Uint64
	cacheHits       atomic.Uint64
	cacheMisses     atomic.Uint64
	rateAllowed     atomic.Uint64
	rateRejected    atomic.Uint64
	reloads         atomic.Uint64
}

// New creates a state component. The valkey peer is resolved at Init, not here.
func New(opts ...Option) *CFState {
	o := options{
		logger:     slog.Default(),
		sessionTTL: defaultSessionTTL,
		cacheTTL:   defaultCacheTTL,
	}
	for _, opt := range opts {
		opt(&o)
	}
	c := &CFState{
		configSource: o.configSource,
		configPath:   o.configPath,
		srcEnvPrefix: o.srcEnvPrefix,
		srcFormat:    o.srcFormat,
		srcFormatSet: o.srcFormatSet,
		sessionTTL:   o.sessionTTL,
		cacheTTL:     o.cacheTTL,
		valkeyName:   o.valkeyName,
		logger:       o.logger,
		loggerSet:    o.loggerSet,
		name:         o.name,
	}
	if o.loaded != nil {
		c.applyConfig(*o.loaded)
	}
	return c
}

// applyConfig overlays non-zero fields of cfg onto the component's base
// settings. It runs last, so a loaded config always wins over option-set
// defaults.
func (c *CFState) applyConfig(cfg StateConfig) {
	if cfg.SessionTTLSec > 0 {
		c.sessionTTL = time.Duration(cfg.SessionTTLSec) * time.Second
	}
	if cfg.CacheTTLSec > 0 {
		c.cacheTTL = time.Duration(cfg.CacheTTLSec) * time.Second
	}
}

// Name implements cf.CaerusComponent. Returns the custom name set via WithName,
// or the default ComponentName ("valkey-state") if no custom name was set.
func (c *CFState) Name() string {
	if c.name != "" {
		return c.name
	}
	return ComponentName
}

// GetInitOrderStage implements cf.CaerusComponent.
func (c *CFState) GetInitOrderStage() cf.Stage { return ComponentStage }

// GetDependencies implements cf.Dependencies. The component depends on the
// valkey component it consumes (the actual peer name when WithValkeyName is
// set, the default ComponentName otherwise), logs through the framework logs
// component, and depends on configuration when WithConfigSource is set. Peer
// names are fixed at construction, so the graph is stable before Init.
func (c *CFState) GetDependencies() []string {
	deps := []string{cf_valkey.ComponentName, cf_logs.ComponentName}
	if c.valkeyName != "" {
		deps[0] = c.valkeyName
	}
	if c.configSource != "" {
		deps = append(deps, cf_configuration.ComponentName)
	}
	return deps
}

// Init implements cf.CaerusComponent. It resolves the valkey peer component
// (by name or the default "valkey"), failing fast when it is missing or not
// yet initialized. No connection is opened here; the peer owns its client.
func (c *CFState) Init(ctx context.Context, fw *cf.CaerusFramework) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.vk != nil {
		return nil // already initialized
	}
	c.fw = fw
	if !c.loggerSet {
		if logs, ok := cf.Get[*cf_logs.Logs](fw); ok {
			c.logsSub = logs.OnReconfigureFor(c.Name(), func(l *slog.Logger) { c.logger = l })
		}
	}
	if c.configSource != "" {
		if err := c.applyConfigFromSource(); err != nil {
			return err
		}
	}
	var vk *cf_valkey.CFValkey
	var ok bool
	if c.valkeyName == "" {
		vk, ok = cf.Get[*cf_valkey.CFValkey](fw)
	} else {
		vk, ok = cf.GetByName[*cf_valkey.CFValkey](fw, c.valkeyName)
	}
	if !ok {
		return fmt.Errorf("cf_valkey_state: valkey component %q is not registered (add it to the framework and to GetDependencies)", c.peerName())
	}
	if vk.Client() == nil {
		return fmt.Errorf("cf_valkey_state: valkey component %q is not initialized", c.peerName())
	}
	c.vk = vk
	return nil
}

// peerName returns the configured valkey peer name, or the valkey
// ComponentName when unset. Callers must not hold the mutex.
func (c *CFState) peerName() string {
	if c.valkeyName != "" {
		return c.valkeyName
	}
	return cf_valkey.ComponentName
}

// applyConfigFromSource reloads the bound configuration source and overlays it
// onto the component's base settings. It must be called with the mutex held.
func (c *CFState) applyConfigFromSource() error {
	conf, ok := cf.Get[*cf_configuration.Configuration](c.fw)
	if !ok {
		return errors.New("cf_valkey_state: configuration component not registered")
	}
	loaded, ok := cf_configuration.Get[StateConfig](conf, c.configSource)
	if !ok {
		return fmt.Errorf("cf_valkey_state: configuration source %q not found", c.configSource)
	}
	c.applyConfig(*loaded)
	return nil
}

// peer returns the resolved valkey peer component (nil before Init or after
// Shutdown).
func (c *CFState) peer() *cf_valkey.CFValkey {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.vk
}

// client returns the peer's live valkey client. It follows the peer-pointer
// convention: the peer is re-read per use, so a client swap on config reload is
// picked up immediately.
func (c *CFState) client() (valkey.Client, error) {
	vk := c.peer()
	if vk == nil {
		return nil, errors.New("cf_valkey_state: component is not initialized")
	}
	cl := vk.Client()
	if cl == nil {
		return nil, errors.New("cf_valkey_state: valkey client is not initialized")
	}
	return cl, nil
}

func (c *CFState) sessionKey(id string) string {
	if vk := c.peer(); vk != nil {
		return vk.Key(nsSession, id)
	}
	return strings.Join([]string{nsSession, id}, ":")
}

func (c *CFState) cacheKey(parts ...string) string {
	if vk := c.peer(); vk != nil {
		return vk.Key(append([]string{nsCache}, parts...)...)
	}
	return strings.Join(append([]string{nsCache}, parts...), ":")
}

func (c *CFState) rlKey(key string) string {
	if vk := c.peer(); vk != nil {
		return vk.Key(nsRateLimit, key)
	}
	return strings.Join([]string{nsRateLimit, key}, ":")
}

// CreateSession stores a JSON session value at session:<id> with the given
// TTL. A ttl <= 0 uses the configured default. The session id is chosen by the
// caller (typically an opaque token); it is never derived from the value.
func (c *CFState) CreateSession(ctx context.Context, id string, v any, ttl time.Duration) error {
	client, err := c.client()
	if err != nil {
		return err
	}
	if id == "" {
		return errors.New("cf_valkey_state: CreateSession: empty session id")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("cf_valkey_state: marshal session: %w", err)
	}
	if ttl <= 0 {
		ttl = c.sessionTTLValue()
	}
	key := c.sessionKey(id)
	if err := client.Do(ctx, client.B().Set().Key(key).Value(string(b)).Px(ttl).Build()).Error(); err != nil {
		return fmt.Errorf("cf_valkey_state: set session %q: %w", id, err)
	}
	c.sessionsCreated.Add(1)
	return nil
}

// GetSession reads the session at session:<id> and unmarshals it into dst.
// found is false when the session does not exist or has expired. The session
// TTL is not extended here; call TouchSession for a sliding window.
func (c *CFState) GetSession(ctx context.Context, id string, dst any) (found bool, err error) {
	client, err := c.client()
	if err != nil {
		return false, err
	}
	if id == "" {
		return false, errors.New("cf_valkey_state: GetSession: empty session id")
	}
	resp := client.Do(ctx, client.B().Get().Key(c.sessionKey(id)).Build())
	if resp.Error() != nil {
		if errors.Is(resp.Error(), valkey.Nil) {
			return false, nil
		}
		return false, resp.Error()
	}
	b, err := resp.AsBytes()
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, dst); err != nil {
		return false, fmt.Errorf("cf_valkey_state: unmarshal session %q: %w", id, err)
	}
	return true, nil
}

// SessionExists reports whether session:<id> exists and is unexpired.
func (c *CFState) SessionExists(ctx context.Context, id string) (bool, error) {
	client, err := c.client()
	if err != nil {
		return false, err
	}
	if id == "" {
		return false, errors.New("cf_valkey_state: SessionExists: empty session id")
	}
	resp := client.Do(ctx, client.B().Exists().Key(c.sessionKey(id)).Build())
	if resp.Error() != nil {
		return false, resp.Error()
	}
	n, err := resp.AsInt64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// TouchSession extends session:<id> with a fresh TTL (sliding window). A
// missing or expired session is a no-op success. A ttl <= 0 uses the configured
// default.
func (c *CFState) TouchSession(ctx context.Context, id string, ttl time.Duration) error {
	client, err := c.client()
	if err != nil {
		return err
	}
	if id == "" {
		return errors.New("cf_valkey_state: TouchSession: empty session id")
	}
	if ttl <= 0 {
		ttl = c.sessionTTLValue()
	}
	resp := client.Do(ctx, client.B().Pexpire().Key(c.sessionKey(id)).Milliseconds(ttl.Milliseconds()).Build())
	if resp.Error() != nil {
		return fmt.Errorf("cf_valkey_state: touch session %q: %w", id, resp.Error())
	}
	n, err := resp.AsInt64()
	if err != nil {
		return err
	}
	if n == 1 {
		c.sessionsTouched.Add(1)
	}
	return nil
}

// RevokeSession deletes session:<id>. Deleting a missing session is a
// no-op success.
func (c *CFState) RevokeSession(ctx context.Context, id string) error {
	client, err := c.client()
	if err != nil {
		return err
	}
	if id == "" {
		return errors.New("cf_valkey_state: RevokeSession: empty session id")
	}
	if err := client.Do(ctx, client.B().Del().Key(c.sessionKey(id)).Build()).Error(); err != nil {
		return fmt.Errorf("cf_valkey_state: revoke session %q: %w", id, err)
	}
	c.sessionsRevoked.Add(1)
	return nil
}

// CacheGet reads a JSON value at cache:<parts...> into dst. A miss returns nil
// and leaves dst untouched. Cache hit/miss counters are updated.
func (c *CFState) CacheGet(ctx context.Context, dst any, parts ...string) error {
	client, err := c.client()
	if err != nil {
		return err
	}
	key := c.cacheKey(parts...)
	resp := client.Do(ctx, client.B().Get().Key(key).Build())
	if resp.Error() != nil {
		if errors.Is(resp.Error(), valkey.Nil) {
			c.cacheMisses.Add(1)
			return nil
		}
		return resp.Error()
	}
	b, err := resp.AsBytes()
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, dst); err != nil {
		return fmt.Errorf("cf_valkey_state: unmarshal cache %q: %w", key, err)
	}
	c.cacheHits.Add(1)
	return nil
}

// CacheSet marshals v as JSON and stores it at cache:<parts...> with the given
// TTL. A ttl <= 0 uses the configured default.
func (c *CFState) CacheSet(ctx context.Context, v any, ttl time.Duration, parts ...string) error {
	client, err := c.client()
	if err != nil {
		return err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("cf_valkey_state: marshal cache: %w", err)
	}
	if ttl <= 0 {
		ttl = c.cacheTTLValue()
	}
	key := c.cacheKey(parts...)
	if err := client.Do(ctx, client.B().Set().Key(key).Value(string(b)).Px(ttl).Build()).Error(); err != nil {
		return fmt.Errorf("cf_valkey_state: set cache %q: %w", key, err)
	}
	return nil
}

// LoadFunc fetches the canonical value on a cache miss. Called at most once
// per key per in-flight wave inside this process.
type LoadFunc func(ctx context.Context) ([]byte, error)

// CacheGetOrLoad returns the bytes cached at cache:<parts...>, or runs load
// once for concurrent callers in this process (singleflight), then stores the
// result with ttl. A ttl <= 0 uses the configured default.
//
// WARNING: singleflight is process-local only; other pods still stampede on a
// cold key. shared=true when this caller waited on another goroutine's load.
// Failure modes: a load error is returned to waiters and nothing is cached; a
// SET failure after a successful load returns the loaded bytes along with the
// error so callers can log without losing the value.
func (c *CFState) CacheGetOrLoad(ctx context.Context, ttl time.Duration, load LoadFunc, parts ...string) (val []byte, shared bool, err error) {
	client, err := c.client()
	if err != nil {
		return nil, false, err
	}
	if load == nil {
		return nil, false, errors.New("cf_valkey_state: CacheGetOrLoad: nil load func")
	}
	if ttl <= 0 {
		ttl = c.cacheTTLValue()
	}
	key := c.cacheKey(parts...)

	resp := client.Do(ctx, client.B().Get().Key(key).Build())
	if resp.Error() == nil {
		b, err := resp.AsBytes()
		if err == nil {
			c.cacheHits.Add(1)
			return b, false, nil
		}
	}
	if resp.Error() != nil && !errors.Is(resp.Error(), valkey.Nil) {
		return nil, false, resp.Error()
	}
	c.cacheMisses.Add(1)

	sfKey := key
	result, err, shared := c.sf.Do(sfKey, func() (any, error) {
		b, lerr := load(ctx)
		if lerr != nil {
			return nil, lerr
		}
		if serr := client.Do(ctx, client.B().Set().Key(key).Value(string(b)).Px(ttl).Build()).Error(); serr != nil {
			return b, serr
		}
		return b, nil
	})
	if result != nil {
		val = result.([]byte)
	}
	if err != nil && val != nil {
		return val, shared, err
	}
	return val, shared, err
}

// RateLimitResult reports the outcome of a RateLimit check.
type RateLimitResult struct {
	// Allowed is true when Count does not exceed the limit for the window.
	Allowed bool
	// Count is the incremented request count within the current window.
	Count int64
	// ResetIn is the time remaining until the window resets (for Retry-After).
	ResetIn time.Duration
}

// RateLimit is an atomic fixed-window rate limiter. Each call increments the
// counter at rl:<key> and reports whether the resulting count is within limit
// for the window. The counter and its expiry are set atomically (Lua), so a
// key never leaks without a TTL and concurrent first calls cannot race the
// expiry.
//
// The key is caller-chosen (e.g. "login:user@example.com" or "ip:1.2.3.4").
// A transport error is returned to the caller: fail-open/fail-closed policy
// belongs to the app (auth-api keeps its "valkey down → allow request" choice).
func (c *CFState) RateLimit(ctx context.Context, key string, limit int64, window time.Duration) (RateLimitResult, error) {
	client, err := c.client()
	if err != nil {
		return RateLimitResult{}, err
	}
	if key == "" {
		return RateLimitResult{}, errors.New("cf_valkey_state: RateLimit: empty key")
	}
	if limit < 0 {
		limit = 0
	}
	if window <= 0 {
		window = time.Second
	}
	resp := client.Do(ctx, client.B().Eval().Script(luaRateLimit).
		Numkeys(1).
		Key(c.rlKey(key)).
		Arg(strconv.FormatInt(window.Milliseconds(), 10)).
		Build())
	if resp.Error() != nil {
		return RateLimitResult{}, resp.Error()
	}
	vals, err := resp.AsIntSlice()
	if err != nil {
		return RateLimitResult{}, err
	}
	res := RateLimitResult{ResetIn: window}
	if len(vals) >= 1 {
		res.Count = vals[0]
	}
	if len(vals) >= 2 && vals[1] > 0 {
		res.ResetIn = time.Duration(vals[1]) * time.Millisecond
	}
	res.Allowed = res.Count <= limit
	if res.Allowed {
		c.rateAllowed.Add(1)
	} else {
		c.rateRejected.Add(1)
	}
	return res, nil
}

// OnConfigReload implements cf.ConfigReloader. It re-reads the behavior
// tunables (session/cache TTLs) from the bound configuration source. No
// connection is rebuilt: the valkey peer owns client rotation, and this
// component is stateless over it.
func (c *CFState) OnConfigReload(source string, cfg any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if source != c.configSource || c.vk == nil || c.fw == nil {
		return
	}
	if _, ok := cfg.(*StateConfig); !ok {
		c.logger.Error("cf_valkey_state: config reload rejected", "source", source, "type", fmt.Sprintf("%T", cfg))
		return
	}
	if err := c.applyConfigFromSource(); err != nil {
		c.logger.Error("cf_valkey_state: config reload rejected; keeping previous", "err", err)
		return
	}
	c.reloads.Add(1)
	c.logger.Info("cf_valkey_state: tunables reloaded",
		"session_ttl", c.sessionTTL.String(),
		"cache_ttl", c.cacheTTL.String(),
	)
}

// RegisterConfigSources implements cf.ConfigSourceRegistrar. The framework
// calls it during argv absorption; it registers this component's configuration
// source (name, path, env prefix, format, Owner) with the configuration
// component. No-op when no source is bound.
func (c *CFState) RegisterConfigSources(conf any) error {
	cfg, ok := conf.(*cf_configuration.Configuration)
	if !ok {
		return fmt.Errorf("cf_valkey_state: RegisterConfigSources: expected configuration component, got %T", conf)
	}
	if c.configSource == "" {
		return nil
	}
	format := c.srcFormat
	if !c.srcFormatSet {
		if p := strings.ToLower(c.configPath); strings.HasSuffix(p, ".yaml") || strings.HasSuffix(p, ".yml") {
			format = cf_configuration.FormatYAML
		} else {
			format = cf_configuration.FormatJSON
		}
	}
	return cf_configuration.AddSource(cfg, cf_configuration.Source[StateConfig]{
		Name:      c.configSource,
		Path:      c.configPath,
		Format:    format,
		Owner:     c.Name(),
		EnvPrefix: c.srcEnvPrefix,
	})
}

// Shutdown implements cf.CaerusComponent. It unsubscribes the logs
// subscription and drops the valkey peer. Further use returns an error.
func (c *CFState) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logsSub != nil {
		c.logsSub.Unsubscribe()
		c.logsSub = nil
	}
	c.vk = nil
	return nil
}

// Client returns the peer's live valkey client (nil before Init or after
// Shutdown). It follows the peer-pointer convention: the peer is re-read per
// use, so a client swap on the valkey side is picked up immediately.
func (c *CFState) Client() valkey.Client {
	vk := c.peer()
	if vk == nil {
		return nil
	}
	return vk.Client()
}

// Key builds a prefix-aware key through the valkey peer. Useful for composing
// with caerus-framework-valkey/patterns helpers (e.g. patterns.NewMutex)
// against the same key space.
func (c *CFState) Key(parts ...string) string {
	if vk := c.peer(); vk != nil {
		return vk.Key(parts...)
	}
	return strings.Join(parts, ":")
}

// Health implements cf.HealthProvider. It reports healthy while the peer's
// valkey client is initialized; real connectivity is owned by the valkey
// component's own Health (aggregated by observability's /readyz).
func (c *CFState) Health(ctx context.Context) error {
	vk := c.peer()
	if vk == nil || vk.Client() == nil {
		return errors.New("cf_valkey_state: valkey client is not initialized")
	}
	return nil
}

// Metrics implements cf_observability.MetricsProvider. It reports operation
// counters while the peer's client is initialized; before Init or after
// Shutdown it returns nil, so the observability component skips it (lazy
// pickup). Counters are cumulative for the process lifetime and are emitted
// (zero until first fired) so the series are always present.
func (c *CFState) Metrics() []cf_observability.Metric {
	if c.Client() == nil {
		return nil
	}
	labels := map[string]string{"component": c.Name()}
	ms := []cf_observability.Metric{
		{
			Name:   "valkey_state_info",
			Help:   "Valkey state component descriptor; 1 while initialized.",
			Value:  1,
			Labels: copyLabels(labels),
		},
		{
			Name:   "valkey_state_config_reloads_total",
			Help:   "Total number of successful tunable reloads after a configuration reload.",
			Value:  float64(c.reloads.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		{
			Name:   "valkey_state_sessions_created_total",
			Help:   "Total number of sessions created.",
			Value:  float64(c.sessionsCreated.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		{
			Name:   "valkey_state_sessions_revoked_total",
			Help:   "Total number of sessions revoked.",
			Value:  float64(c.sessionsRevoked.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		{
			Name:   "valkey_state_sessions_touched_total",
			Help:   "Total number of sliding-window session touches.",
			Value:  float64(c.sessionsTouched.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		{
			Name:   "valkey_state_cache_hits_total",
			Help:   "Total number of cache hits.",
			Value:  float64(c.cacheHits.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		{
			Name:   "valkey_state_cache_misses_total",
			Help:   "Total number of cache misses.",
			Value:  float64(c.cacheMisses.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		{
			Name:   "valkey_state_rate_allowed_total",
			Help:   "Total number of rate-limit checks that stayed within the limit.",
			Value:  float64(c.rateAllowed.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		{
			Name:   "valkey_state_rate_rejected_total",
			Help:   "Total number of rate-limit checks that exceeded the limit.",
			Value:  float64(c.rateRejected.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
	}
	return ms
}

// sessionTTLValue returns the effective default session TTL under the lock.
func (c *CFState) sessionTTLValue() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessionTTL
}

// cacheTTLValue returns the effective default cache TTL under the lock.
func (c *CFState) cacheTTLValue() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheTTL
}

// copyLabels returns a shallow copy of a label map so callers cannot mutate
// the component's internal state.
func copyLabels(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

var _ cf.CaerusComponent = (*CFState)(nil)
var _ cf.Dependencies = (*CFState)(nil)
var _ cf.HealthProvider = (*CFState)(nil)
var _ cf_observability.MetricsProvider = (*CFState)(nil)
var _ cf.ConfigReloader = (*CFState)(nil)
var _ cf.ConfigSourceRegistrar = (*CFState)(nil)
