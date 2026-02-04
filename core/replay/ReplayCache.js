/**
 * QuantLab Replay Cache Layer v1
 * 
 * Production-grade caching with TTL and bounded LRU.
 * Follows cache-discipline: Cache only accelerates, never source of truth.
 */

class ReplayCache {
  constructor(options = {}) {
    this.maxSize = options.maxSize || 1000;
    this.ttl = options.ttl || 60000; // Default 1 min
    this.cache = new Map();
    this.metrics = {
      hits: 0,
      misses: 0,
      evictions: 0
    };
  }

  /**
   * Get a value from cache. 
   * Returns null if not found or expired.
   */
  get(key) {
    const entry = this.cache.get(key);
    if (!entry) {
      this.metrics.misses++;
      return null;
    }

    if (Date.now() > entry.expiry) {
      this.cache.delete(key);
      this.metrics.misses++;
      return null;
    }

    this.metrics.hits++;
    // Move to end (LRU)
    this.cache.delete(key);
    this.cache.set(key, entry);
    return entry.value;
  }

  /**
   * Set a value in cache with TTL.
   * Ensures bounded size by evicting oldest entry if maxSize is reached.
   */
  set(key, value, ttlOverride = null) {
    if (this.cache.size >= this.maxSize) {
      // Evict the oldest entry (first inserted in Map)
      const oldestKey = this.cache.keys().next().value;
      this.cache.delete(oldestKey);
      this.metrics.evictions++;
    }

    const ttl = ttlOverride || this.ttl;
    this.cache.set(key, {
      value,
      expiry: Date.now() + ttl
    });
  }

  invalidate(key) {
    this.cache.delete(key);
  }

  invalidateByPrefix(prefix) {
    if (!prefix) return;
    for (const key of this.cache.keys()) {
      if (key.startsWith(prefix)) this.cache.delete(key);
    }
  }

  invalidateAll() {
    this.cache.clear();
  }

  getMetrics() {
    return { ...this.metrics, size: this.cache.size };
  }
}

// Global singletons for different segments
export const metaCache = new ReplayCache({ maxSize: 500, ttl: 300000 }); // 5 min for meta
export const filesCache = new ReplayCache({ maxSize: 1000, ttl: 60000 }); // 1 min for file lists
export const pageCache = new ReplayCache({ maxSize: 2000, ttl: 15000 });  // 15 sec for data pages
