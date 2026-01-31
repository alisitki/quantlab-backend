/**
 * DecisionBuilder — deterministic promotion decision from triad report.
 */

export const DECISION_THRESHOLDS = {
  min_shadow_confidence_delta: 0.01,
  max_histogram_l1_risk: 200,
  max_calibration_avg_proba_l1_risk: 1.0,
  max_calibration_win_rate_l1_risk: 1.0,
  max_weight_used: 1.2
};

export class DecisionBuilder {
  build(report) {
    const reasons = [];
    if (!report) {
      return this.#decision('HOLD_SHADOW', ['missing_report'], DECISION_THRESHOLDS);
    }

    const verdict = report?.shadow_vs_off?.verdict || null;
    const activeApplied = report?.active_vs_shadow?.active_applied === true;
    const activeReason = report?.active_vs_shadow?.active_reason || null;
    const violations = report?.active_vs_shadow?.audit?.violations_count || 0;
    const maxWeightUsed = report?.active_vs_shadow?.audit?.max_weight_used ?? null;
    const confidenceDelta = report?.shadow_vs_off?.obs_delta?.confidence_mean_delta ?? null;

    const riskHistogram = report?.shadow_vs_off?.obs_delta?.histogram_l1 || null;
    const riskCalibrationAvg = report?.shadow_vs_off?.obs_delta?.calibration_avg_proba_l1 || null;
    const riskCalibrationWin = report?.shadow_vs_off?.obs_delta?.calibration_win_rate_l1 || null;

    if (verdict !== 'EDGE_VAR') {
      reasons.push('verdict_not_edge_var');
      return this.#decision('HOLD_SHADOW', reasons, DECISION_THRESHOLDS);
    }

    if (!activeApplied) {
      if (activeReason) reasons.push(`active_not_applied:${activeReason}`);
      return this.#decision('HOLD_SHADOW', reasons, DECISION_THRESHOLDS);
    }

    if (violations > 0) {
      reasons.push('active_audit_violations');
      return this.#decision('HOLD_SHADOW', reasons, DECISION_THRESHOLDS);
    }

    if (maxWeightUsed !== null && maxWeightUsed > DECISION_THRESHOLDS.max_weight_used) {
      reasons.push('max_weight_exceeded');
      return this.#decision('HOLD_SHADOW', reasons, DECISION_THRESHOLDS);
    }

    if (
      (riskHistogram !== null && riskHistogram > DECISION_THRESHOLDS.max_histogram_l1_risk) ||
      (riskCalibrationAvg !== null && riskCalibrationAvg > DECISION_THRESHOLDS.max_calibration_avg_proba_l1_risk) ||
      (riskCalibrationWin !== null && riskCalibrationWin > DECISION_THRESHOLDS.max_calibration_win_rate_l1_risk)
    ) {
      reasons.push('risk_threshold_exceeded');
      return this.#decision('REJECT', reasons, DECISION_THRESHOLDS);
    }

    if (confidenceDelta === null || confidenceDelta < DECISION_THRESHOLDS.min_shadow_confidence_delta) {
      reasons.push('shadow_signal_below_min');
      return this.#decision('HOLD_SHADOW', reasons, DECISION_THRESHOLDS);
    }

    return this.#decision('PROMOTE_ACTIVE', ['edge_var_confirmed'], DECISION_THRESHOLDS);
  }

  #decision(value, reasons, thresholds) {
    return {
      decision: value,
      reasons: reasons,
      thresholds_used: thresholds
    };
  }
}
