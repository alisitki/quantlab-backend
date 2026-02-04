/**
 * manifestWriter — helpers to attach optional extras.
 */

export function attachMlExtra(manifest, mlResult, options = {}) {
  if (!manifest) return manifest;

  const extra = manifest.extra && typeof manifest.extra === 'object' ? manifest.extra : {};
  extra.ml = {
    mode: options.mode || 'off',
    model_type: mlResult?.model_type || null,
    model_version: mlResult?.model_version || null,
    proba: mlResult?.proba ?? null,
    confidence: mlResult?.confidence ?? null,
    regime: mlResult?.regime ?? null,
    active_applied: options.active_applied === true,
    active_reason: options.active_reason || null,
    applied_weight: options.applied_weight ?? null,
    active_mode: options.active_mode === true,
    job_id: options.job_id || null,
    job_hash: options.job_hash || null,
    decision_path: options.decision_path || null
  };

  manifest.extra = extra;
  return manifest;
}

export function ensureLastCursor(manifest, cursor) {
  if (!manifest || !manifest.output) return manifest;
  if (manifest.output.last_cursor) return manifest;
  const value = cursor || 'CURSOR_NONE';
  manifest.output.last_cursor = value;
  return manifest;
}
