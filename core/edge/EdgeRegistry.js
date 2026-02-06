/**
 * EdgeRegistry: Manages edge lifecycle and discovery
 *
 * Responsibilities:
 * - Register and store edges
 * - Filter active edges by regime
 * - Track edge performance
 * - Retire underperforming edges
 * - Persist edges to disk
 */
export class EdgeRegistry {
  #edges = new Map();
  #edgeDefinitions = new Map(); // Store original definitions for reconstruction

  constructor() {
    this.#edges = new Map();
    this.#edgeDefinitions = new Map();
  }

  /**
   * Register a new edge
   * @param {Edge} edge - Edge instance
   * @param {Object} definition - Original edge definition (for persistence)
   */
  register(edge, definition = null) {
    this.#edges.set(edge.id, edge);

    if (definition) {
      this.#edgeDefinitions.set(edge.id, definition);
    }
  }

  /**
   * Get edge by ID
   * @param {string} id - Edge ID
   * @returns {Edge|undefined}
   */
  get(id) {
    return this.#edges.get(id);
  }

  /**
   * Get all edges
   * @returns {Array<Edge>}
   */
  getAll() {
    return Array.from(this.#edges.values());
  }

  /**
   * Get edges by status
   * @param {string} status - CANDIDATE, VALIDATED, DEPLOYED, RETIRED
   * @returns {Array<Edge>}
   */
  getByStatus(status) {
    return this.getAll().filter(e => e.status === status);
  }

  /**
   * Get active edges for current features and regime
   * @param {Object} features - Feature vector
   * @param {string|number} regime - Current regime
   * @returns {Array<Object>} Array of { edge, evaluation }
   */
  getActiveEdges(features, regime) {
    const active = [];

    for (const edge of this.#edges.values()) {
      // Skip retired edges
      if (edge.status === 'RETIRED') continue;

      const evaluation = edge.evaluateEntry(features, regime);

      if (evaluation.active) {
        active.push({ edge, evaluation });
      }
    }

    return active;
  }

  /**
   * Get edges valid for a specific regime
   * @param {string|number} regime - Regime label
   * @returns {Array<Edge>}
   */
  getByRegime(regime) {
    return this.getAll().filter(edge => {
      // If edge has no regime constraints, it's valid everywhere
      if (!edge.regimes) return true;

      // Otherwise check if regime is in the list
      return edge.regimes.includes(regime);
    });
  }

  /**
   * Update edge with trade result
   * @param {string} edgeId - Edge ID
   * @param {Object} trade - Trade result
   */
  updateEdgeStats(edgeId, trade) {
    const edge = this.#edges.get(edgeId);

    if (!edge) {
      console.warn(`Edge ${edgeId} not found for stats update`);
      return;
    }

    edge.updateStats(trade);

    // Auto-retire if needed
    if (edge.shouldRetire() && edge.status !== 'RETIRED') {
      console.log(`Edge ${edge.id} auto-retired due to poor performance`);
      edge.status = 'RETIRED';
    }
  }

  /**
   * Remove edge from registry
   * @param {string} id - Edge ID
   * @returns {boolean} True if removed
   */
  remove(id) {
    const removed = this.#edges.delete(id);
    this.#edgeDefinitions.delete(id);
    return removed;
  }

  /**
   * Get registry statistics
   */
  getStats() {
    const edges = this.getAll();

    const stats = {
      total: edges.length,
      byStatus: {
        CANDIDATE: 0,
        VALIDATED: 0,
        DEPLOYED: 0,
        RETIRED: 0
      },
      totalTrades: 0,
      avgHealthScore: 0
    };

    for (const edge of edges) {
      stats.byStatus[edge.status] = (stats.byStatus[edge.status] || 0) + 1;
      stats.totalTrades += edge.stats.trades;
    }

    const healthScores = edges.map(e => e.getHealthScore());
    stats.avgHealthScore = healthScores.reduce((sum, s) => sum + s, 0) / (healthScores.length || 1);

    return stats;
  }

  /**
   * Retire edges that should be retired
   * @returns {Array<string>} IDs of retired edges
   */
  retireUnderperformingEdges() {
    const retired = [];

    for (const edge of this.#edges.values()) {
      if (edge.status !== 'RETIRED' && edge.shouldRetire()) {
        edge.status = 'RETIRED';
        retired.push(edge.id);
        console.log(`Edge ${edge.id} (${edge.name}) retired: health score ${edge.getHealthScore().toFixed(2)}`);
      }
    }

    return retired;
  }

  /**
   * Serialize registry for persistence
   */
  toJSON() {
    const edges = [];

    for (const [id, edge] of this.#edges) {
      edges.push({
        ...edge.toJSON(),
        definition: this.#edgeDefinitions.get(id) || null
      });
    }

    return {
      edges,
      stats: this.getStats(),
      timestamp: Date.now()
    };
  }

  /**
   * Clear all edges (for testing)
   */
  clear() {
    this.#edges.clear();
    this.#edgeDefinitions.clear();
  }

  /**
   * Get count of edges
   */
  size() {
    return this.#edges.size;
  }
}
