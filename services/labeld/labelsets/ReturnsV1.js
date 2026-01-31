/**
 * ReturnsV1 — deterministic future return labels.
 */

export const LABEL_SET_ID = 'returns';
export const LABEL_SET_VERSION = 'v1';

const ROUND_SCALE = 1e8;

export function buildReturnsLabels(rows, horizonMs) {
  if (!Array.isArray(rows) || rows.length === 0) return [];

  const labels = new Array(rows.length);
  let j = 0;
  const horizon = BigInt(Math.max(0, Math.trunc(horizonMs)));

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const ts = row.ts_event;

    if (j < i) j = i;

    const target = ts + horizon;
    while (j < rows.length && rows[j].ts_event < target) {
      j += 1;
    }

    if (j >= rows.length) {
      labels[i] = { label_future_return: null, label_direction: null };
      continue;
    }

    const future = rows[j];
    if (!Number.isFinite(row.f_mid_price) || !Number.isFinite(future.f_mid_price) || row.f_mid_price === 0) {
      labels[i] = { label_future_return: null, label_direction: null };
      continue;
    }

    const ret = (future.f_mid_price - row.f_mid_price) / row.f_mid_price;
    const rounded = round(ret);
    let direction = 0;
    if (rounded > 0) direction = 1;
    else if (rounded < 0) direction = -1;

    labels[i] = { label_future_return: rounded, label_direction: direction };
  }

  return labels;
}

function round(value) {
  if (!Number.isFinite(value)) return null;
  return Math.round(value * ROUND_SCALE) / ROUND_SCALE;
}
