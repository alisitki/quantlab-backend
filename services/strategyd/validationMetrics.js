/**
 * Validation metrics for snapshot→tick pipeline.
 */

const counters = {
  strategyd_validation_triggered_total: 0,
  strategyd_validation_diverged_total: 0
};

export function incrementValidationTriggered() {
  counters.strategyd_validation_triggered_total += 1;
}

export function incrementValidationDiverged() {
  counters.strategyd_validation_diverged_total += 1;
}

export function getValidationMetrics() {
  return { ...counters };
}
