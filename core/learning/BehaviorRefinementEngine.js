/**
 * BehaviorRefinementEngine: Generate behavior refinement proposals from feature importance
 *
 * Purpose: Translate feature importance analysis into actionable refinement proposals.
 *
 * Proposal Types:
 * 1. WEIGHT_ADJUST: High-importance features → threshold/condition adjustment
 * 2. PRUNE_CANDIDATE: Low-importance features across multiple edges → remove
 * 3. NEW_FEATURE_SIGNAL: High win correlation but unused in edges → add to discovery
 *
 * SAFETY: Proposals are NOT auto-applied. Human review required.
 */

export class BehaviorRefinementEngine {
  #proposalHistory = [];
  #config = {};

  /**
   * @param {Object} config
   * @param {number} config.highImportanceThreshold - Threshold for WEIGHT_ADJUST (default: 0.6)
   * @param {number} config.lowImportanceThreshold - Threshold for PRUNE_CANDIDATE (default: 0.15)
   * @param {number} config.minEdgesForPrune - Min edges showing low importance (default: 3)
   * @param {number} config.newFeatureCorrelation - Min correlation for NEW_FEATURE_SIGNAL (default: 0.5)
   */
  constructor(config = {}) {
    this.#config = {
      highImportanceThreshold: config.highImportanceThreshold || 0.6,
      lowImportanceThreshold: config.lowImportanceThreshold || 0.15,
      minEdgesForPrune: config.minEdgesForPrune || 3,
      newFeatureCorrelation: config.newFeatureCorrelation || 0.5
    };
  }

  /**
   * Generate refinement proposals from importance analysis
   * @param {Object} importanceData - { edgeId: { feature: { importance, correlation, ... } } }
   * @param {EdgeRegistry} edgeRegistry - Edge registry to check feature usage
   * @returns {Array<RefinementProposal>}
   *
   * RefinementProposal = {
   *   type: 'WEIGHT_ADJUST'|'PRUNE_CANDIDATE'|'NEW_FEATURE_SIGNAL',
   *   edgeId: string|null,
   *   featureName: string,
   *   reasoning: string,
   *   data: Object,
   *   timestamp: number,
   *   priority: 'HIGH'|'MEDIUM'|'LOW'
   * }
   */
  generateProposals(importanceData, edgeRegistry) {
    if (!importanceData || Object.keys(importanceData).length === 0) {
      return [];
    }

    const proposals = [];

    // 1. Generate WEIGHT_ADJUST proposals (high importance)
    proposals.push(...this.#generateWeightAdjustProposals(importanceData, edgeRegistry));

    // 2. Generate PRUNE_CANDIDATE proposals (low importance across edges)
    proposals.push(...this.#generatePruneProposals(importanceData, edgeRegistry));

    // 3. Generate NEW_FEATURE_SIGNAL proposals (unused but correlated)
    proposals.push(...this.#generateNewFeatureProposals(importanceData, edgeRegistry));

    // Sort by priority
    proposals.sort((a, b) => {
      const priorityOrder = { HIGH: 0, MEDIUM: 1, LOW: 2 };
      return priorityOrder[a.priority] - priorityOrder[b.priority];
    });

    // Store in history
    this.#proposalHistory.push({
      timestamp: Date.now(),
      proposals
    });

    return proposals;
  }

  /**
   * Generate WEIGHT_ADJUST proposals
   * High-importance features → suggest tighter thresholds or increased weight
   */
  #generateWeightAdjustProposals(importanceData, edgeRegistry) {
    const proposals = [];

    for (const [edgeId, featureImportance] of Object.entries(importanceData)) {
      const edge = edgeRegistry.get(edgeId);
      if (!edge) continue;

      // Find high-importance features
      const highImportanceFeatures = Object.entries(featureImportance)
        .filter(([_, stats]) => stats.importance >= this.#config.highImportanceThreshold)
        .sort((a, b) => b[1].importance - a[1].importance);

      for (const [featureName, stats] of highImportanceFeatures) {
        // Check if feature is used in edge definition
        const usedInEdge = this.#isFeatureUsedInEdge(edge, featureName);

        let reasoning;
        let suggestedAction;
        let priority;

        if (usedInEdge) {
          // Feature is already used - suggest threshold refinement
          reasoning = `Feature "${featureName}" shows high importance (${stats.importance.toFixed(2)}) for edge "${edgeId}". Current threshold may need adjustment.`;
          suggestedAction = 'REFINE_THRESHOLD';
          priority = 'MEDIUM';
        } else {
          // Feature not used but important - suggest adding
          reasoning = `Feature "${featureName}" shows high importance (${stats.importance.toFixed(2)}) but is not used in edge "${edgeId}" definition. Consider adding as condition.`;
          suggestedAction = 'ADD_CONDITION';
          priority = 'HIGH';
        }

        proposals.push({
          type: 'WEIGHT_ADJUST',
          edgeId,
          featureName,
          reasoning,
          data: {
            importance: stats.importance,
            correlation: stats.correlation,
            pValue: stats.pValue,
            suggestedAction,
            currentlyUsed: usedInEdge
          },
          timestamp: Date.now(),
          priority
        });
      }
    }

    return proposals;
  }

  /**
   * Generate PRUNE_CANDIDATE proposals
   * Low-importance features across multiple edges → suggest removal
   */
  #generatePruneProposals(importanceData, edgeRegistry) {
    const proposals = [];

    // Count low importance across edges
    const featureLowCounts = {};

    for (const [edgeId, featureImportance] of Object.entries(importanceData)) {
      for (const [featureName, stats] of Object.entries(featureImportance)) {
        if (stats.importance < this.#config.lowImportanceThreshold) {
          if (!featureLowCounts[featureName]) {
            featureLowCounts[featureName] = { count: 0, edges: [], avgImportance: 0, sum: 0 };
          }
          featureLowCounts[featureName].count++;
          featureLowCounts[featureName].edges.push(edgeId);
          featureLowCounts[featureName].sum += stats.importance;
        }
      }
    }

    // Generate proposals for features with low importance across many edges
    for (const [featureName, data] of Object.entries(featureLowCounts)) {
      if (data.count >= this.#config.minEdgesForPrune) {
        const avgImportance = data.sum / data.count;

        proposals.push({
          type: 'PRUNE_CANDIDATE',
          edgeId: null, // Applies to multiple edges
          featureName,
          reasoning: `Feature "${featureName}" shows low importance (avg: ${avgImportance.toFixed(2)}) across ${data.count} edges. May be noise.`,
          data: {
            avgImportance,
            edgeCount: data.count,
            edges: data.edges
          },
          timestamp: Date.now(),
          priority: data.count >= 5 ? 'HIGH' : 'MEDIUM'
        });
      }
    }

    return proposals;
  }

  /**
   * Generate NEW_FEATURE_SIGNAL proposals
   * Features with high correlation but not used in any edge
   */
  #generateNewFeatureProposals(importanceData, edgeRegistry) {
    const proposals = [];

    // Collect all features and their statistics
    const featureStats = {};

    for (const [edgeId, featureImportance] of Object.entries(importanceData)) {
      const edge = edgeRegistry.get(edgeId);
      if (!edge) continue;

      for (const [featureName, stats] of Object.entries(featureImportance)) {
        if (!featureStats[featureName]) {
          featureStats[featureName] = {
            correlations: [],
            usedInEdges: 0,
            edgeCount: 0
          };
        }

        featureStats[featureName].correlations.push(stats.correlation);
        featureStats[featureName].edgeCount++;

        if (this.#isFeatureUsedInEdge(edge, featureName)) {
          featureStats[featureName].usedInEdges++;
        }
      }
    }

    // Find features with strong correlation but low usage
    for (const [featureName, stats] of Object.entries(featureStats)) {
      const avgCorrelation = stats.correlations.reduce((sum, c) => sum + Math.abs(c), 0) / stats.correlations.length;

      // Feature is strongly correlated but not used in most edges
      if (avgCorrelation >= this.#config.newFeatureCorrelation && stats.usedInEdges < stats.edgeCount * 0.3) {
        proposals.push({
          type: 'NEW_FEATURE_SIGNAL',
          edgeId: null,
          featureName,
          reasoning: `Feature "${featureName}" shows strong correlation (avg: ${avgCorrelation.toFixed(2)}) but is only used in ${stats.usedInEdges}/${stats.edgeCount} edges. Consider adding to discovery scan.`,
          data: {
            avgCorrelation,
            usageRate: stats.usedInEdges / stats.edgeCount,
            edgeCount: stats.edgeCount
          },
          timestamp: Date.now(),
          priority: avgCorrelation > 0.7 ? 'HIGH' : 'MEDIUM'
        });
      }
    }

    return proposals;
  }

  /**
   * Check if feature is used in edge definition
   */
  #isFeatureUsedInEdge(edge, featureName) {
    if (!edge.definition?.pattern?.conditions) {
      return false;
    }

    return edge.definition.pattern.conditions.some(
      cond => cond.feature === featureName
    );
  }

  /**
   * Get proposal history
   */
  getHistory() {
    return this.#proposalHistory;
  }

  /**
   * Get latest proposals
   */
  getLatestProposals() {
    if (this.#proposalHistory.length === 0) return [];
    return this.#proposalHistory[this.#proposalHistory.length - 1].proposals;
  }

  /**
   * Get proposals by type
   */
  getProposalsByType(type) {
    const latest = this.getLatestProposals();
    return latest.filter(p => p.type === type);
  }

  /**
   * Get high-priority proposals
   */
  getHighPriorityProposals() {
    const latest = this.getLatestProposals();
    return latest.filter(p => p.priority === 'HIGH');
  }

  /**
   * Serialize to JSON
   */
  toJSON() {
    return {
      config: this.#config,
      proposalHistory: this.#proposalHistory
    };
  }

  /**
   * Deserialize from JSON
   */
  static fromJSON(json) {
    const engine = new BehaviorRefinementEngine(json.config);
    engine.#proposalHistory = json.proposalHistory || [];
    return engine;
  }

  /**
   * Get summary statistics
   */
  getSummary() {
    const latest = this.getLatestProposals();

    const typeCounts = {
      WEIGHT_ADJUST: 0,
      PRUNE_CANDIDATE: 0,
      NEW_FEATURE_SIGNAL: 0
    };

    const priorityCounts = {
      HIGH: 0,
      MEDIUM: 0,
      LOW: 0
    };

    for (const proposal of latest) {
      typeCounts[proposal.type]++;
      priorityCounts[proposal.priority]++;
    }

    return {
      totalProposals: latest.length,
      byType: typeCounts,
      byPriority: priorityCounts,
      historySize: this.#proposalHistory.length
    };
  }
}
