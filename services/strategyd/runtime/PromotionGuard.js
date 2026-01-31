/**
 * PromotionGuard — validates PROMOTE_ACTIVE decision against triad report.
 */

export class PromotionGuard {
  evaluate(decision, report) {
    if (!decision || !report) {
      return { allowed: false, reason: 'missing_inputs' };
    }
    if (decision.decision !== 'PROMOTE_ACTIVE') {
      return { allowed: false, reason: decision.decision || 'not_promoted' };
    }
    if (!decision.thresholds_used || typeof decision.thresholds_used !== 'object') {
      return { allowed: false, reason: 'missing_thresholds' };
    }
    const activeApplied = report?.active_vs_shadow?.active_applied === true;
    const decisionExplicit = Array.isArray(decision.reasons)
      ? decision.reasons.includes('edge_var_confirmed')
      : false;
    if (!activeApplied && !decisionExplicit) {
      return { allowed: false, reason: 'active_not_applied' };
    }
    const violations = report?.active_vs_shadow?.audit?.violations_count || 0;
    if (violations > 0) {
      return { allowed: false, reason: 'safety_violation' };
    }
    return { allowed: true, reason: 'promote_active' };
  }
}
