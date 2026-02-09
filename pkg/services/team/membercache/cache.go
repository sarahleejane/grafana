package membercache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/infra/tracing"
)

var logger = log.New("team.membercache")

// Cache provides sync tracking to prevent redundant team synchronization operations
// It tracks when users were last synced to reduce database load
type Cache interface {
	// ShouldSync returns true if the user should be synced (not recently synced)
	ShouldSync(ctx context.Context, orgID, userID int64) bool

	// MarkSynced marks a user as having been synced at the current time
	MarkSynced(ctx context.Context, orgID, userID int64)

	// ClearUser removes sync tracking for a specific user (e.g., on login)
	ClearUser(ctx context.Context, userID int64)
}

type cacheImpl struct {
	cache  *expirable.LRU[string, time.Time]
	tracer tracing.Tracer
	ttl    time.Duration

	// userKeys maps userID to all their cache keys (across orgs) for efficient clearing
	userKeys map[int64][]string
	mu       sync.RWMutex
}

// NewCache creates a new user sync tracking cache with LRU+TTL eviction
func NewCache(maxSize int, ttl time.Duration, tracer tracing.Tracer) Cache {
	logger.Info("Initializing user sync tracking cache", "maxSize", maxSize, "ttl", ttl)

	cache := expirable.NewLRU(
		maxSize,
		func(key string, value time.Time) {
			// Eviction callback - log when entries are evicted
			logger.Debug("Cache entry evicted", "key", key)
		},
		ttl,
	)

	return &cacheImpl{
		cache:    cache,
		tracer:   tracer,
		ttl:      ttl,
		userKeys: make(map[int64][]string),
	}
}

func (c *cacheImpl) ShouldSync(ctx context.Context, orgID, userID int64) bool {
	_, span := c.tracer.Start(ctx, "team.membercache.ShouldSync", trace.WithAttributes(
		attribute.Int64("org_id", orgID),
		attribute.Int64("user_id", userID),
	))
	defer span.End()

	key := makeCacheKey(orgID, userID)
	lastSync, found := c.cache.Get(key)

	if found {
		timeSinceSync := time.Since(lastSync)
		shouldSync := timeSinceSync >= c.ttl

		span.SetAttributes(
			attribute.Bool("cache.hit", true),
			attribute.Int64("time_since_sync_ms", timeSinceSync.Milliseconds()),
			attribute.Bool("should_sync", shouldSync),
		)

		if !shouldSync {
			logger.Debug("User recently synced, skipping",
				"key", key,
				"lastSync", lastSync,
				"timeSince", timeSinceSync)
			return false
		}
	}

	span.SetAttributes(attribute.Bool("cache.hit", false))
	logger.Debug("User should be synced", "key", key, "found", found)
	return true
}

func (c *cacheImpl) MarkSynced(ctx context.Context, orgID, userID int64) {
	_, span := c.tracer.Start(ctx, "team.membercache.MarkSynced", trace.WithAttributes(
		attribute.Int64("org_id", orgID),
		attribute.Int64("user_id", userID),
	))
	defer span.End()

	key := makeCacheKey(orgID, userID)
	now := time.Now()
	c.cache.Add(key, now)

	// Track this key for the user to enable efficient clearing
	c.mu.Lock()
	keys := c.userKeys[userID]
	// Check if key already exists
	found := false
	for _, k := range keys {
		if k == key {
			found = true
			break
		}
	}
	if !found {
		c.userKeys[userID] = append(keys, key)
	}
	c.mu.Unlock()

	logger.Debug("Marked user as synced", "key", key, "time", now)
}

func (c *cacheImpl) ClearUser(ctx context.Context, userID int64) {
	_, span := c.tracer.Start(ctx, "team.membercache.ClearUser", trace.WithAttributes(
		attribute.Int64("user_id", userID),
	))
	defer span.End()

	c.mu.Lock()
	defer c.mu.Unlock()

	keys, found := c.userKeys[userID]
	if !found {
		logger.Debug("No cache entries found for user", "userID", userID)
		return
	}

	// Remove all keys for this user (across all orgs)
	for _, key := range keys {
		c.cache.Remove(key)
		logger.Debug("Cleared cache entry", "userID", userID, "key", key)
	}
	delete(c.userKeys, userID)

	logger.Debug("Cleared all cache entries for user", "userID", userID, "count", len(keys))
}

// makeCacheKey creates a cache key for org-user sync tracking
func makeCacheKey(orgID, userID int64) string {
	return fmt.Sprintf("%d:%d", orgID, userID)
}

// NoOpCache is a cache implementation that does nothing (used when caching is disabled)
type NoOpCache struct{}

func (n *NoOpCache) ShouldSync(ctx context.Context, orgID, userID int64) bool {
	return true // Always sync when cache disabled
}

func (n *NoOpCache) MarkSynced(ctx context.Context, orgID, userID int64) {
	// No-op
}

func (n *NoOpCache) ClearUser(ctx context.Context, userID int64) {
	// No-op
}
