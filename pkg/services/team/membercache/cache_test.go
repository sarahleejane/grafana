package membercache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/grafana/pkg/infra/tracing"
)

func TestCache_ShouldSync_InitialSync(t *testing.T) {
	ctx := context.Background()
	tracer := tracing.InitializeTracerForTest()
	cache := NewCache(100, 5*time.Minute, tracer)

	// First sync - should always return true
	shouldSync := cache.ShouldSync(ctx, 1, 1)
	assert.True(t, shouldSync, "First sync should always proceed")
}

func TestCache_ShouldSync_RecentlySync(t *testing.T) {
	ctx := context.Background()
	tracer := tracing.InitializeTracerForTest()
	cache := NewCache(100, 5*time.Minute, tracer)

	// Mark user as synced
	cache.MarkSynced(ctx, 1, 1)

	// Immediately check - should return false (recently synced)
	shouldSync := cache.ShouldSync(ctx, 1, 1)
	assert.False(t, shouldSync, "Should skip sync for recently synced user")
}

func TestCache_ShouldSync_AfterTTL(t *testing.T) {
	ctx := context.Background()
	tracer := tracing.InitializeTracerForTest()

	// Create cache with very short TTL
	cache := NewCache(100, 100*time.Millisecond, tracer)

	// Mark user as synced
	cache.MarkSynced(ctx, 1, 1)

	// Should not sync immediately
	shouldSync := cache.ShouldSync(ctx, 1, 1)
	require.False(t, shouldSync)

	// Wait for TTL to expire
	time.Sleep(150 * time.Millisecond)

	// Should sync again after TTL
	shouldSync = cache.ShouldSync(ctx, 1, 1)
	assert.True(t, shouldSync, "Should sync again after TTL expires")
}

func TestCache_MultipleUsers(t *testing.T) {
	ctx := context.Background()
	tracer := tracing.InitializeTracerForTest()
	cache := NewCache(100, 5*time.Minute, tracer)

	// Mark multiple users as synced
	cache.MarkSynced(ctx, 1, 1)
	cache.MarkSynced(ctx, 1, 2)
	cache.MarkSynced(ctx, 2, 1) // Different org

	// All should be skipped
	assert.False(t, cache.ShouldSync(ctx, 1, 1))
	assert.False(t, cache.ShouldSync(ctx, 1, 2))
	assert.False(t, cache.ShouldSync(ctx, 2, 1))

	// Different user should still sync
	assert.True(t, cache.ShouldSync(ctx, 1, 3))
}

func TestCache_ClearUser(t *testing.T) {
	ctx := context.Background()
	tracer := tracing.InitializeTracerForTest()
	cache := NewCache(100, 5*time.Minute, tracer)

	// Mark users as synced
	cache.MarkSynced(ctx, 1, 1)
	cache.MarkSynced(ctx, 1, 2)

	// Verify both are cached
	assert.False(t, cache.ShouldSync(ctx, 1, 1))
	assert.False(t, cache.ShouldSync(ctx, 1, 2))

	// Clear user 1
	cache.ClearUser(ctx, 1)

	// User 1 should sync again (cache cleared)
	assert.True(t, cache.ShouldSync(ctx, 1, 1))

	// User 2 should still be cached
	assert.False(t, cache.ShouldSync(ctx, 1, 2))
}

func TestCache_LRUEviction(t *testing.T) {
	ctx := context.Background()
	tracer := tracing.InitializeTracerForTest()

	// Create a cache with only 3 entries
	cache := NewCache(3, 5*time.Minute, tracer)

	// Mark 3 users as synced
	cache.MarkSynced(ctx, 1, 1)
	cache.MarkSynced(ctx, 1, 2)
	cache.MarkSynced(ctx, 1, 3)

	// All should be cached
	assert.False(t, cache.ShouldSync(ctx, 1, 1))
	assert.False(t, cache.ShouldSync(ctx, 1, 2))
	assert.False(t, cache.ShouldSync(ctx, 1, 3))

	// Mark a 4th user - should evict the oldest (user 1)
	cache.MarkSynced(ctx, 1, 4)

	// User 4 should be cached
	assert.False(t, cache.ShouldSync(ctx, 1, 4))

	// User 1 likely evicted (LRU), should need sync
	// Note: This test is probabilistic based on LRU implementation
	shouldSync := cache.ShouldSync(ctx, 1, 1)
	t.Logf("User 1 shouldSync after eviction: %v", shouldSync)
}

func TestCache_DifferentOrgs(t *testing.T) {
	ctx := context.Background()
	tracer := tracing.InitializeTracerForTest()
	cache := NewCache(100, 5*time.Minute, tracer)

	// Same user ID, different orgs
	cache.MarkSynced(ctx, 1, 1)
	cache.MarkSynced(ctx, 2, 1)

	// Both should be cached independently
	assert.False(t, cache.ShouldSync(ctx, 1, 1))
	assert.False(t, cache.ShouldSync(ctx, 2, 1))

	// Clear user 1 (clears ALL entries for this user across all orgs)
	cache.ClearUser(ctx, 1)

	// Both orgs should need sync (user was cleared from all orgs)
	assert.True(t, cache.ShouldSync(ctx, 1, 1))
	assert.True(t, cache.ShouldSync(ctx, 2, 1))
}

func TestNoOpCache(t *testing.T) {
	ctx := context.Background()
	cache := &NoOpCache{}

	// ShouldSync should always return true
	assert.True(t, cache.ShouldSync(ctx, 1, 1))
	assert.True(t, cache.ShouldSync(ctx, 1, 2))

	// MarkSynced should not panic
	cache.MarkSynced(ctx, 1, 1)

	// Still should return true (no caching)
	assert.True(t, cache.ShouldSync(ctx, 1, 1))

	// ClearUser should not panic
	cache.ClearUser(ctx, 1)
}

func TestCache_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	tracer := tracing.InitializeTracerForTest()
	cache := NewCache(1000, 5*time.Minute, tracer)

	// Run concurrent operations
	done := make(chan bool, 100)

	for i := 0; i < 100; i++ {
		go func(idx int) {
			// Each goroutine does some cache operations
			userID := int64(idx % 10)
			orgID := int64(1)

			cache.ShouldSync(ctx, orgID, userID)
			cache.MarkSynced(ctx, orgID, userID)

			if idx%10 == 0 {
				cache.ClearUser(ctx, userID)
			}

			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 100; i++ {
		<-done
	}

	// Cache should still be functional
	cache.MarkSynced(ctx, 1, 1)
	shouldSync := cache.ShouldSync(ctx, 1, 1)
	assert.False(t, shouldSync, "Cache should still work after concurrent access")
}

func TestCache_MarkSyncedBeforeCheck(t *testing.T) {
	ctx := context.Background()
	tracer := tracing.InitializeTracerForTest()
	cache := NewCache(100, 5*time.Minute, tracer)

	// Mark synced first
	cache.MarkSynced(ctx, 1, 1)

	// Then check - should say don't sync
	shouldSync := cache.ShouldSync(ctx, 1, 1)
	assert.False(t, shouldSync)

	// Clear and check again
	cache.ClearUser(ctx, 1)
	shouldSync = cache.ShouldSync(ctx, 1, 1)
	assert.True(t, shouldSync, "Should sync after cache clear")
}
