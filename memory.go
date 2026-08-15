package cf_valkey_state

import (
	"sync"
	"sync/atomic"
	"time"
)

const defaultMemoryMaxEntries = 10000

// MapFullPolicy is what happens when the counter sticky-note map hits MaxEntries
// for a new logical key.
type MapFullPolicy int

const (
	// MapFullAllow allows a new key without counting when the map is full.
	MapFullAllow MapFullPolicy = iota
	// MapFullDeny rejects Allow for a new key when the cap is reached.
	MapFullDeny
)

type memEntry struct {
	count       int64
	windowStart time.Time
	window      time.Duration
}

type memoryLimiter struct {
	mu        sync.Mutex
	entries   map[string]*memEntry
	fullAllow atomic.Uint64
	fullDeny  atomic.Uint64
}

func newMemoryLimiter() *memoryLimiter {
	return &memoryLimiter{entries: make(map[string]*memEntry)}
}

func (m *memoryLimiter) sweepExpiredLocked(now time.Time) {
	for k, e := range m.entries {
		if now.Sub(e.windowStart) >= e.window {
			delete(m.entries, k)
		}
	}
}

func (m *memoryLimiter) Allow(key string, limit int64, window time.Duration, maxEntries int, whenFull MapFullPolicy) (RateLimitResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if maxEntries <= 0 {
		return RateLimitResult{Allowed: true}, nil
	}
	now := time.Now()
	m.sweepExpiredLocked(now)
	e, ok := m.entries[key]
	if !ok {
		if len(m.entries) >= maxEntries {
			if whenFull == MapFullDeny {
				m.fullDeny.Add(1)
				return RateLimitResult{Allowed: false}, nil
			}
			m.fullAllow.Add(1)
			return RateLimitResult{Allowed: true}, nil
		}
		e = &memEntry{windowStart: now, window: window}
		m.entries[key] = e
	}
	if now.Sub(e.windowStart) >= e.window {
		e.count = 0
		e.windowStart = now
		e.window = window
	}
	e.count++
	res := RateLimitResult{Count: e.count, ResetIn: e.windowStart.Add(e.window).Sub(now)}
	res.Allowed = res.Count <= limit
	return res, nil
}

func (m *memoryLimiter) Peek(key string) RateLimitResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	e, ok := m.entries[key]
	if !ok {
		return RateLimitResult{}
	}
	if now.Sub(e.windowStart) >= e.window {
		delete(m.entries, key)
		return RateLimitResult{}
	}
	return RateLimitResult{Count: e.count, ResetIn: e.windowStart.Add(e.window).Sub(now)}
}

func (m *memoryLimiter) Reset(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, key)
}

func (m *memoryLimiter) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

func (m *memoryLimiter) FullAllow() uint64 { return m.fullAllow.Load() }

func (m *memoryLimiter) FullDeny() uint64 { return m.fullDeny.Load() }
