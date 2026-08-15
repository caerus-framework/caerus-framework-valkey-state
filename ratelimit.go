package cf_valkey_state

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/valkey-io/valkey-go"
)

const luaPeek = `local c = redis.call("GET", KEYS[1])
local t = redis.call("PTTL", KEYS[1])
if not c then
  return {0, 0}
end
return {tonumber(c), t}`

// ErrMissingTTL is returned when a Valkey counter exists with count > 0 but no
// usable TTL, after the key was scrubbed, and the missing-TTL policy is error.
var ErrMissingTTL = errors.New("cf_valkey_state: counter missing TTL")

// ErrInvalidWindow is returned when window is not positive.
var ErrInvalidWindow = errors.New("cf_valkey_state: window must be > 0")

// ErrInvalidLimit is returned when limit is not positive (the HTTP limiter
// may disable limiting before calling the machine; the store never treats
// limit 0 as “always allow”).
var ErrInvalidLimit = errors.New("cf_valkey_state: limit must be > 0")

// MissingTTLPolicy selects what happens after scrubbing a counter that has
// count > 0 but no usable TTL. Zero value means proceed.
type MissingTTLPolicy int

const (
	// MissingTTLProceed continues after delete: Allow retries Lua once; Peek
	// returns empty. This is the default when CounterOpts leaves the field unset.
	MissingTTLProceed MissingTTLPolicy = iota
	// MissingTTLError returns ErrMissingTTL after delete.
	MissingTTLError
)

// CounterOpts is optional per-call store hygiene. The app does not pick
// Valkey vs memory here — that is the nested rate_limit settings.
type CounterOpts struct {
	// MissingTTL overrides the default proceed policy for this call.
	MissingTTL MissingTTLPolicy
}

// RateLimitResult reports the outcome of Allow or Peek.
type RateLimitResult struct {
	// Allowed is true when Count does not exceed the limit for the window.
	// Peek always leaves this false (Peek has no limit).
	Allowed bool
	// Count is the (incremented, for Allow) request count within the window.
	Count int64
	// ResetIn is the time remaining until the window resets (for Retry-After).
	ResetIn time.Duration
}

// RateLimit is sugar for Allow with default CounterOpts (fixed window).
func (c *CFState) RateLimit(ctx context.Context, key string, limit int64, window time.Duration) (RateLimitResult, error) {
	return c.Allow(ctx, key, limit, window)
}

// Allow increments the fixed-window counter for the logical key.
func (c *CFState) Allow(ctx context.Context, key string, limit int64, window time.Duration) (RateLimitResult, error) {
	return c.AllowOpts(ctx, key, limit, window, CounterOpts{})
}

// AllowOpts is Allow with a missing-TTL override.
func (c *CFState) AllowOpts(ctx context.Context, key string, limit int64, window time.Duration, opts CounterOpts) (RateLimitResult, error) {
	if err := c.validateCounterArgs(key, limit, window); err != nil {
		return RateLimitResult{}, err
	}
	if c.countersOnMemory() {
		return c.allowMemory(key, limit, window)
	}
	res, err := c.allowValkey(ctx, key, limit, window, opts.MissingTTL, false)
	if err != nil && c.useMemoryFallbackValue() {
		c.logger.Warn("cf_valkey_state: valkey counter failed; using memory", "err", err)
		return c.allowMemory(key, limit, window)
	}
	return res, err
}

// Peek reads the counter without incrementing. Allowed is always false.
func (c *CFState) Peek(ctx context.Context, key string) (RateLimitResult, error) {
	return c.PeekOpts(ctx, key, CounterOpts{})
}

// PeekOpts is Peek with a missing-TTL override.
func (c *CFState) PeekOpts(ctx context.Context, key string, opts CounterOpts) (RateLimitResult, error) {
	if key == "" {
		return RateLimitResult{}, errors.New("cf_valkey_state: empty key")
	}
	if c.countersOnMemory() {
		return c.memory.Peek(key), nil
	}
	res, err := c.peekValkey(ctx, key, opts.MissingTTL)
	if err != nil && c.useMemoryFallbackValue() {
		c.logger.Warn("cf_valkey_state: valkey peek failed; using memory", "err", err)
		return c.memory.Peek(key), nil
	}
	return res, err
}

// Reset deletes the counter for the logical key (idempotent).
func (c *CFState) Reset(ctx context.Context, key string) error {
	if key == "" {
		return errors.New("cf_valkey_state: empty key")
	}
	if c.countersOnMemory() {
		c.memory.Reset(key)
		return nil
	}
	client, err := c.client()
	if err != nil {
		if c.useMemoryFallbackValue() {
			c.memory.Reset(key)
			return nil
		}
		return err
	}
	if err := client.Do(ctx, client.B().Del().Key(c.rlKey(key)).Build()).Error(); err != nil {
		if c.useMemoryFallbackValue() {
			c.memory.Reset(key)
			return nil
		}
		return fmt.Errorf("cf_valkey_state: reset failed: %w", err)
	}
	return nil
}

func (c *CFState) validateCounterArgs(key string, limit int64, window time.Duration) error {
	if key == "" {
		return errors.New("cf_valkey_state: empty key")
	}
	if limit <= 0 {
		return ErrInvalidLimit
	}
	if window <= 0 {
		return ErrInvalidWindow
	}
	return nil
}

func (c *CFState) countersOnMemory() bool {
	c.mu.RLock()
	force := c.forceMemory
	fallback := c.useMemoryFallback
	c.mu.RUnlock()
	if force {
		return true
	}
	_, err := c.client()
	if err != nil {
		return fallback
	}
	return false
}

func (c *CFState) useMemoryFallbackValue() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.useMemoryFallback
}

func (c *CFState) allowMemory(key string, limit int64, window time.Duration) (RateLimitResult, error) {
	c.mu.RLock()
	max := c.memoryMaxEntries
	when := c.mapFullPolicy
	c.mu.RUnlock()
	if max <= 0 {
		max = defaultMemoryMaxEntries
	}
	res, err := c.memory.Allow(key, limit, window, max, when)
	if err != nil {
		return RateLimitResult{}, err
	}
	if res.Allowed {
		c.rateAllowed.Add(1)
	} else {
		c.rateRejected.Add(1)
	}
	c.rateMemoryPath.Add(1)
	return res, nil
}

func (c *CFState) allowValkey(ctx context.Context, key string, limit int64, window time.Duration, ttlPolicy MissingTTLPolicy, retried bool) (RateLimitResult, error) {
	client, err := c.client()
	if err != nil {
		return RateLimitResult{}, err
	}
	storeKey := c.rlKey(key)
	resp := client.Do(ctx, client.B().Eval().Script(luaRateLimit).
		Numkeys(1).
		Key(storeKey).
		Arg(strconv.FormatInt(window.Milliseconds(), 10)).
		Build())
	if resp.Error() != nil {
		return RateLimitResult{}, resp.Error()
	}
	vals, err := resp.AsIntSlice()
	if err != nil {
		return RateLimitResult{}, err
	}
	var count, pttl int64
	if len(vals) >= 1 {
		count = vals[0]
	}
	if len(vals) >= 2 {
		pttl = vals[1]
	}
	if count > 0 && pttl <= 0 {
		if err := c.handleMissingTTL(ctx, client, storeKey, ttlPolicy); err != nil {
			return RateLimitResult{}, err
		}
		if retried {
			return RateLimitResult{}, ErrMissingTTL
		}
		return c.allowValkey(ctx, key, limit, window, ttlPolicy, true)
	}
	res := RateLimitResult{Count: count, ResetIn: window, Allowed: count <= limit}
	if pttl > 0 {
		res.ResetIn = time.Duration(pttl) * time.Millisecond
	}
	if res.Allowed {
		c.rateAllowed.Add(1)
	} else {
		c.rateRejected.Add(1)
	}
	return res, nil
}

func (c *CFState) peekValkey(ctx context.Context, key string, ttlPolicy MissingTTLPolicy) (RateLimitResult, error) {
	client, err := c.client()
	if err != nil {
		return RateLimitResult{}, err
	}
	storeKey := c.rlKey(key)
	resp := client.Do(ctx, client.B().Eval().Script(luaPeek).
		Numkeys(1).
		Key(storeKey).
		Build())
	if resp.Error() != nil {
		return RateLimitResult{}, resp.Error()
	}
	vals, err := resp.AsIntSlice()
	if err != nil {
		return RateLimitResult{}, err
	}
	var count, pttl int64
	if len(vals) >= 1 {
		count = vals[0]
	}
	if len(vals) >= 2 {
		pttl = vals[1]
	}
	if count > 0 && pttl <= 0 {
		if err := c.handleMissingTTL(ctx, client, storeKey, ttlPolicy); err != nil {
			return RateLimitResult{}, err
		}
		return RateLimitResult{}, nil
	}
	res := RateLimitResult{Count: count}
	if pttl > 0 {
		res.ResetIn = time.Duration(pttl) * time.Millisecond
	}
	return res, nil
}

func (c *CFState) handleMissingTTL(ctx context.Context, client valkey.Client, storeKey string, ttlPolicy MissingTTLPolicy) error {
	c.rateMissingTTL.Add(1)
	c.logger.Error("cf_valkey_state: counter missing TTL; scrubbing key")
	_ = client.Do(ctx, client.B().Del().Key(storeKey).Build()).Error()
	if ttlPolicy == MissingTTLError {
		return ErrMissingTTL
	}
	return nil
}
