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
	nsSession     = "session"
	nsSessionBind = "session-bind"
	nsSessionUser = "session-user"
	nsCache       = "cache"
	nsRateLimit   = "rl"
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

// luaSessionCreate writes the payload, optional user bind, and SADD on the
// per-user SET. KEYS: payload, bind, user-set. ARGV: json, ttlMs, sessionId, userId.
// Empty userId skips bind and SET (same as a session with no index).
const luaSessionCreate = `redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
if ARGV[4] ~= "" then
  redis.call("SET", KEYS[2], ARGV[4], "PX", ARGV[2])
  redis.call("SADD", KEYS[3], ARGV[3])
  local t = tonumber(ARGV[2])
  local cur = redis.call("PTTL", KEYS[3])
  if cur < t then
    redis.call("PEXPIRE", KEYS[3], t)
  end
end
return 1`

// luaSessionRevoke deletes payload + bind and SREM from the user SET when a
// bind exists. KEYS: payload, bind. ARGV: sessionId, userSetPrefix (ends with :).
const luaSessionRevoke = `local user = redis.call("GET", KEYS[2])
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
if type(user) == "string" and user ~= "" then
  redis.call("SREM", ARGV[2] .. user, ARGV[1])
end
return 1`

// luaSessionTouch extends payload TTL; if a bind exists, the same TTL on the
// bind and at least that TTL on the user SET. KEYS: payload, bind.
// ARGV: ttlMs, userSetPrefix.
const luaSessionTouch = `local n = redis.call("PEXPIRE", KEYS[1], ARGV[1])
if redis.call("EXISTS", KEYS[2]) == 1 then
  redis.call("PEXPIRE", KEYS[2], ARGV[1])
  local user = redis.call("GET", KEYS[2])
  if type(user) == "string" and user ~= "" then
    local uk = ARGV[2] .. user
    local t = tonumber(ARGV[1])
    local cur = redis.call("PTTL", uk)
    if cur < t then
      redis.call("PEXPIRE", uk, t)
    end
  end
end
return n`

// luaSessionRevokeAll drops every member of the user SET except keep ids.
// KEYS: user-set. ARGV: sessionPrefix, bindPrefix, keep...
const luaSessionRevokeAll = `local sp = ARGV[1]
local bp = ARGV[2]
local keep = {}
for i = 3, #ARGV do
  keep[ARGV[i]] = true
end
local ids = redis.call("SMEMBERS", KEYS[1])
local n = 0
for _, id in ipairs(ids) do
  if not keep[id] then
    redis.call("DEL", sp .. id)
    redis.call("DEL", bp .. id)
    redis.call("SREM", KEYS[1], id)
    n = n + 1
  end
end
return n`

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
	// RateLimit is the nested counter-store section (Lua vs sticky-note map).
	// HTTP limit/window numbers do not live here. Env overlay does not walk
	// nested structs — use the flat RATE_LIMIT_* env tags on StateConfig
	// (README → Configuration → Env example) or set rate_limit in the file.
	RateLimit RateLimitConfig `json:"rate_limit,omitempty" yaml:"rate_limit,omitempty" env:"-"`

	// Env overlay cannot recurse into nested structs; these aliases merge
	// into RateLimit in applyConfig (VALKEY_STATE_RATE_LIMIT_*).
	RateLimitUseMemoryFallback *bool  `json:"-" yaml:"-" env:"RATE_LIMIT_USE_MEMORY_FALLBACK"`
	RateLimitForceMemory       *bool  `json:"-" yaml:"-" env:"RATE_LIMIT_FORCE_MEMORY"`
	RateLimitMemoryMaxEntries  int    `json:"-" yaml:"-" env:"RATE_LIMIT_MEMORY_MAX_ENTRIES"`
	RateLimitMapFullPolicy     string `json:"-" yaml:"-" env:"RATE_LIMIT_MEMORY_MAP_FULL_POLICY"`
}

// RateLimitConfig is the counter store (not HTTP policy). Only this nested
// block may use an in-process map — sessions and cache stay Valkey-only.
type RateLimitConfig struct {
	UseMemoryFallback   *bool  `json:"use_memory_fallback,omitempty" yaml:"use_memory_fallback,omitempty"`
	ForceMemory         *bool  `json:"force_memory,omitempty" yaml:"force_memory,omitempty"`
	MemoryMaxEntries    int    `json:"memory_max_entries,omitempty" yaml:"memory_max_entries,omitempty"`
	MemoryMapFullPolicy string `json:"memory_map_full_policy,omitempty" yaml:"memory_map_full_policy,omitempty"`
}

// Option configures the state component at construction time.
type Option func(*options)

type options struct {
	loaded            *StateConfig // set by WithConfig; overrides option-set defaults
	configSource      string       // named configuration source for live reload
	configPath        string       // source file path (module self-registration)
	srcEnvPrefix      string       // source env overlay prefix (default: NAME_)
	srcFormat         cf_configuration.Format
	srcFormatSet      bool
	sessionTTL        time.Duration
	cacheTTL          time.Duration
	valkeyName        string // valkey peer component name; empty means ComponentName
	logger            *slog.Logger
	loggerSet         bool   // true when WithLogger was called explicitly
	name              string // custom component name; empty means use ComponentName
	requireValkey     bool
	useMemoryFallback *bool
	forceMemory       *bool
	memoryMaxEntries  int
	memoryMapFull     string
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
//	cf_valkey_state.New(cf_valkey_state.WithConfigSource("valkey-state", "config/valkey-state.json"))
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

// WithoutValkeyPeer omits valkey from GetDependencies (GH App / counters-only
// sticky notes). Init then requires rate_limit memory (use_memory_fallback or
// force_memory) plus memory_map_full_policy. Sessions and cache error: there
// is no fridge.
func WithoutValkeyPeer() Option {
	return func(o *options) { o.requireValkey = false }
}

// WithUseMemoryFallback enables the counter sticky-note map when Valkey
// Eval fails or Client() is nil.
func WithUseMemoryFallback(enabled bool) Option {
	return func(o *options) { o.useMemoryFallback = &enabled }
}

// WithForceMemory prefers the counter map even when a live Valkey client exists.
func WithForceMemory(enabled bool) Option {
	return func(o *options) { o.forceMemory = &enabled }
}

// WithMemoryMapFullPolicy sets "allow" or "deny" when the counter map is full.
func WithMemoryMapFullPolicy(policy string) Option {
	return func(o *options) { o.memoryMapFull = policy }
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
	rateMemoryPath  atomic.Uint64
	rateMissingTTL  atomic.Uint64
	reloads         atomic.Uint64

	initialized       atomic.Bool
	requireValkey     bool
	useMemoryFallback bool
	forceMemory       bool
	memoryMaxEntries  int
	mapFullPolicy     MapFullPolicy
	mapFullPolicySet  bool
	memory            *memoryLimiter
}

// New creates a state component. The valkey peer is resolved at Init, not here.
func New(opts ...Option) *CFState {
	o := options{
		logger:        slog.Default(),
		sessionTTL:    defaultSessionTTL,
		cacheTTL:      defaultCacheTTL,
		requireValkey: true,
	}
	for _, opt := range opts {
		opt(&o)
	}
	c := &CFState{
		configSource:     o.configSource,
		configPath:       o.configPath,
		srcEnvPrefix:     o.srcEnvPrefix,
		srcFormat:        o.srcFormat,
		srcFormatSet:     o.srcFormatSet,
		sessionTTL:       o.sessionTTL,
		cacheTTL:         o.cacheTTL,
		valkeyName:       o.valkeyName,
		logger:           o.logger,
		loggerSet:        o.loggerSet,
		name:             o.name,
		requireValkey:    o.requireValkey,
		memoryMaxEntries: defaultMemoryMaxEntries,
		memory:           newMemoryLimiter(),
	}
	if o.useMemoryFallback != nil {
		c.useMemoryFallback = *o.useMemoryFallback
	}
	if o.forceMemory != nil {
		c.forceMemory = *o.forceMemory
	}
	if o.memoryMaxEntries > 0 {
		c.memoryMaxEntries = o.memoryMaxEntries
	}
	if o.memoryMapFull != "" {
		c.applyMapFullPolicy(o.memoryMapFull)
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
	rl := cfg.RateLimit
	if cfg.RateLimitUseMemoryFallback != nil {
		rl.UseMemoryFallback = cfg.RateLimitUseMemoryFallback
	}
	if cfg.RateLimitForceMemory != nil {
		rl.ForceMemory = cfg.RateLimitForceMemory
	}
	if cfg.RateLimitMemoryMaxEntries > 0 {
		rl.MemoryMaxEntries = cfg.RateLimitMemoryMaxEntries
	}
	if cfg.RateLimitMapFullPolicy != "" {
		rl.MemoryMapFullPolicy = cfg.RateLimitMapFullPolicy
	}
	if rl.UseMemoryFallback != nil {
		c.useMemoryFallback = *rl.UseMemoryFallback
	}
	if rl.ForceMemory != nil {
		c.forceMemory = *rl.ForceMemory
	}
	if rl.MemoryMaxEntries > 0 {
		c.memoryMaxEntries = rl.MemoryMaxEntries
	}
	if rl.MemoryMapFullPolicy != "" {
		c.applyMapFullPolicy(rl.MemoryMapFullPolicy)
	}
}

func (c *CFState) applyMapFullPolicy(s string) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "allow":
		c.mapFullPolicy = MapFullAllow
		c.mapFullPolicySet = true
	case "deny":
		c.mapFullPolicy = MapFullDeny
		c.mapFullPolicySet = true
	}
}

func (c *CFState) validateRateLimitMemory() error {
	needMap := c.useMemoryFallback || c.forceMemory || !c.requireValkey
	if !c.requireValkey && !c.useMemoryFallback && !c.forceMemory {
		return errors.New(`cf_valkey_state: WithoutValkeyPeer requires rate_limit.use_memory_fallback or force_memory`)
	}
	if needMap && (c.useMemoryFallback || c.forceMemory) && !c.mapFullPolicySet {
		return errors.New(`cf_valkey_state: rate_limit.memory_map_full_policy is required ("allow" or "deny") when counter memory is on`)
	}
	return nil
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
	deps := []string{cf_logs.ComponentName}
	if c.requireValkey {
		peer := cf_valkey.ComponentName
		if c.valkeyName != "" {
			peer = c.valkeyName
		}
		deps = append([]string{peer}, deps...)
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
	if c.initialized.Load() {
		return nil
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
	if err := c.validateRateLimitMemory(); err != nil {
		return err
	}
	if !c.requireValkey {
		c.logger.Warn("cf_valkey_state: running without a valkey peer (counter sticky-note chassis) — not shared across replicas; sessions/cache will error")
		c.initialized.Store(true)
		return nil
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
	c.vk = vk
	if vk.Client() == nil {
		c.logger.Error("cf_valkey_state: valkey peer has no live client (DegradedMode or not yet connected); Init continues; sessions/cache error until Client() is live; counters follow rate_limit memory settings")
	}
	if c.forceMemory && vk.Client() != nil {
		c.logger.Error("cf_valkey_state: lame_memory_mode — force_memory on while valkey client is live; sticky-note counters are weaker than shared Valkey")
	}
	c.initialized.Store(true)
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
	c.applyConfig(loaded)
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
	return c.nsKey(nsSession, id)
}

func (c *CFState) sessionBindKey(id string) string {
	return c.nsKey(nsSessionBind, id)
}

func (c *CFState) sessionUserKey(userID string) string {
	return c.nsKey(nsSessionUser, userID)
}

func (c *CFState) sessionPrefix() string {
	return c.nsKey(nsSession) + ":"
}

func (c *CFState) sessionBindPrefix() string {
	return c.nsKey(nsSessionBind) + ":"
}

func (c *CFState) sessionUserPrefix() string {
	return c.nsKey(nsSessionUser) + ":"
}

func (c *CFState) nsKey(parts ...string) string {
	if vk := c.peer(); vk != nil {
		return vk.Key(parts...)
	}
	return strings.Join(parts, ":")
}

func (c *CFState) cacheKey(parts ...string) string {
	return c.nsKey(append([]string{nsCache}, parts...)...)
}

func (c *CFState) rlKey(key string) string {
	return c.nsKey(nsRateLimit, key)
}

// SessionOption configures CreateSession (user index binding).
type SessionOption func(*sessionOpts)

type sessionOpts struct {
	userID string
}

// WithSessionUser indexes this session under userID so ListSessionsForUser and
// RevokeAllForUser can find it. Empty userID is ignored (no index, same as
// omitting the option). This is not inferred from the JSON value.
func WithSessionUser(userID string) SessionOption {
	return func(o *sessionOpts) { o.userID = userID }
}

// CreateSession stores a JSON session value at session:<id> with the given
// TTL. A ttl <= 0 uses the configured default. The session id is chosen by the
// caller (typically an opaque token); it is never derived from the value.
// Pass WithSessionUser to maintain the per-user SET (Choice A). Without it,
// the session exists by id only — ListSessionsForUser will not see it.
func (c *CFState) CreateSession(ctx context.Context, id string, v any, ttl time.Duration, opts ...SessionOption) error {
	if id == "" {
		return errors.New("cf_valkey_state: CreateSession: empty session id")
	}
	so := sessionOpts{}
	for _, opt := range opts {
		opt(&so)
	}
	client, err := c.client()
	if err != nil {
		return err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("cf_valkey_state: marshal session: %w", err)
	}
	if ttl <= 0 {
		ttl = c.sessionTTLValue()
	}
	resp := client.Do(ctx, client.B().Eval().Script(luaSessionCreate).
		Numkeys(3).
		Key(c.sessionKey(id), c.sessionBindKey(id), c.sessionUserKey(so.userID)).
		Arg(string(b)).
		Arg(strconv.FormatInt(ttl.Milliseconds(), 10)).
		Arg(id).
		Arg(so.userID).
		Build())
	if err := resp.Error(); err != nil {
		return fmt.Errorf("cf_valkey_state: set session: %w", err)
	}
	c.sessionsCreated.Add(1)
	return nil
}

// GetSession reads the session at session:<id> and unmarshals it into dst.
// found is false when the session does not exist or has expired. The session
// TTL is not extended here; call TouchSession for a sliding window.
func (c *CFState) GetSession(ctx context.Context, id string, dst any) (found bool, err error) {
	if id == "" {
		return false, errors.New("cf_valkey_state: GetSession: empty session id")
	}
	client, err := c.client()
	if err != nil {
		return false, err
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
		return false, fmt.Errorf("cf_valkey_state: unmarshal session: %w", err)
	}
	return true, nil
}

// SessionExists reports whether session:<id> exists and is unexpired.
func (c *CFState) SessionExists(ctx context.Context, id string) (bool, error) {
	if id == "" {
		return false, errors.New("cf_valkey_state: SessionExists: empty session id")
	}
	client, err := c.client()
	if err != nil {
		return false, err
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
	if id == "" {
		return errors.New("cf_valkey_state: TouchSession: empty session id")
	}
	client, err := c.client()
	if err != nil {
		return err
	}
	if ttl <= 0 {
		ttl = c.sessionTTLValue()
	}
	resp := client.Do(ctx, client.B().Eval().Script(luaSessionTouch).
		Numkeys(2).
		Key(c.sessionKey(id), c.sessionBindKey(id)).
		Arg(strconv.FormatInt(ttl.Milliseconds(), 10)).
		Arg(c.sessionUserPrefix()).
		Build())
	if resp.Error() != nil {
		return fmt.Errorf("cf_valkey_state: touch session: %w", resp.Error())
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

// RevokeSession deletes session:<id> and SREM from the user SET when the
// session was created with WithSessionUser. Deleting a missing session is a
// no-op success.
func (c *CFState) RevokeSession(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("cf_valkey_state: RevokeSession: empty session id")
	}
	client, err := c.client()
	if err != nil {
		return err
	}
	if err := client.Do(ctx, client.B().Eval().Script(luaSessionRevoke).
		Numkeys(2).
		Key(c.sessionKey(id), c.sessionBindKey(id)).
		Arg(id).
		Arg(c.sessionUserPrefix()).
		Build()).Error(); err != nil {
		return fmt.Errorf("cf_valkey_state: revoke session: %w", err)
	}
	c.sessionsRevoked.Add(1)
	return nil
}

// ListSessionsForUser returns live session ids for userID (SMEMBERS on the
// per-user SET, then EXISTS on each payload). Ghost members (expired payload)
// are SREM'd. This is O(sessions of that user), not KEYS. Sessions created
// without WithSessionUser are not listed. Empty userID is an error. Errors do
// not include the user id.
func (c *CFState) ListSessionsForUser(ctx context.Context, userID string) ([]string, error) {
	if userID == "" {
		return nil, errors.New("cf_valkey_state: ListSessionsForUser: empty user id")
	}
	client, err := c.client()
	if err != nil {
		return nil, err
	}
	resp := client.Do(ctx, client.B().Smembers().Key(c.sessionUserKey(userID)).Build())
	if resp.Error() != nil {
		return nil, fmt.Errorf("cf_valkey_state: list sessions: %w", resp.Error())
	}
	members, err := resp.AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("cf_valkey_state: list sessions: %w", err)
	}
	live := make([]string, 0, len(members))
	for _, id := range members {
		ex := client.Do(ctx, client.B().Exists().Key(c.sessionKey(id)).Build())
		if ex.Error() != nil {
			return nil, fmt.Errorf("cf_valkey_state: list sessions: %w", ex.Error())
		}
		n, err := ex.AsInt64()
		if err != nil {
			return nil, fmt.Errorf("cf_valkey_state: list sessions: %w", err)
		}
		if n == 1 {
			live = append(live, id)
			continue
		}
		_ = client.Do(ctx, client.B().Srem().Key(c.sessionUserKey(userID)).Member(id).Build())
		_ = client.Do(ctx, client.B().Del().Key(c.sessionBindKey(id)).Build())
	}
	return live, nil
}

// RevokeAllForUser deletes every indexed session for userID except keep.
// keep is typically the current session id ("revoke others"). Not KEYS.
// Empty userID is an error. Errors do not include the user id.
func (c *CFState) RevokeAllForUser(ctx context.Context, userID string, keep ...string) error {
	if userID == "" {
		return errors.New("cf_valkey_state: RevokeAllForUser: empty user id")
	}
	client, err := c.client()
	if err != nil {
		return err
	}
	args := []string{c.sessionPrefix(), c.sessionBindPrefix()}
	args = append(args, keep...)
	eval := client.B().Eval().Script(luaSessionRevokeAll).
		Numkeys(1).
		Key(c.sessionUserKey(userID)).
		Arg(args[0])
	for _, a := range args[1:] {
		eval = eval.Arg(a)
	}
	resp := client.Do(ctx, eval.Build())
	if err := resp.Error(); err != nil {
		return fmt.Errorf("cf_valkey_state: revoke sessions: %w", err)
	}
	n, err := resp.AsInt64()
	if err != nil {
		return fmt.Errorf("cf_valkey_state: revoke sessions: %w", err)
	}
	if n > 0 {
		c.sessionsRevoked.Add(uint64(n))
	}
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
		setClient, serr := c.client()
		if serr != nil {
			return b, serr
		}
		if serr := setClient.Do(ctx, setClient.B().Set().Key(key).Value(string(b)).Px(ttl).Build()).Error(); serr != nil {
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

// OnConfigReload implements cf.ConfigReloader. It re-reads the behavior
// tunables (session/cache TTLs) from the bound configuration source. No
// connection is rebuilt: the valkey peer owns client rotation, and this
// component is stateless over it.
func (c *CFState) OnConfigReload(source string, cfg any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if source != c.configSource || !c.initialized.Load() || c.fw == nil {
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
	c.initialized.Store(false)
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
	if !c.initialized.Load() {
		return errors.New("cf_valkey_state: component is not initialized")
	}
	if !c.requireValkey {
		return nil
	}
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
	if !c.initialized.Load() {
		return nil
	}
	labels := map[string]string{"component": c.Name()}
	bool01 := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}
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
		{
			Name:   "valkey_state_rate_memory_path_total",
			Help:   "Total number of counter Allow calls served from the in-process map.",
			Value:  float64(c.rateMemoryPath.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		{
			Name:   "valkey_state_rate_missing_ttl_total",
			Help:   "Times a Valkey counter was scrubbed for count>0 and PTTL<=0.",
			Value:  float64(c.rateMissingTTL.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		{
			Name:   "valkey_state_rate_force_memory",
			Help:   "1 when rate_limit.force_memory is on.",
			Value:  bool01(c.forceMemory),
			Labels: copyLabels(labels),
		},
		{
			Name:   "valkey_state_rate_use_memory_fallback",
			Help:   "1 when rate_limit.use_memory_fallback is on.",
			Value:  bool01(c.useMemoryFallback),
			Labels: copyLabels(labels),
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
