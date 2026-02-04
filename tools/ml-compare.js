#!/usr/bin/env node
/**
 * ML Model Comparison Tool
 *
 * Compares two ML models' training metrics and decision configurations.
 * Generates a detailed comparison report.
 *
 * Usage:
 *   node tools/ml-compare.js --job-a <jobId> --job-b <jobId> [--output json|text]
 *
 * Environment:
 *   ML_ARTIFACTS_DIR - Path to ML artifacts (default: core/ml/artifacts/jobs)
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_ARTIFACTS_DIR = path.resolve(__dirname, '../core/ml/artifacts/jobs');

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const key = argv[i];
    if (!key.startsWith('--')) continue;
    const name = key.replace(/^--/, '').replace(/-/g, '_');
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args[name] = true;
    } else {
      args[name] = next;
      i++;
    }
  }
  return args;
}

async function readJson(filePath) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function loadModelArtifacts(jobId, artifactsDir) {
  const jobDir = path.join(artifactsDir, jobId);

  const decision = await readJson(path.join(jobDir, 'decision.json'));
  const metrics = await readJson(path.join(jobDir, 'metrics.json'));
  const job = await readJson(path.join(jobDir, 'job.json'));

  return { decision, metrics, job, exists: !!(decision || metrics || job) };
}

function compareMetrics(metricsA, metricsB) {
  if (!metricsA && !metricsB) return null;

  const fields = [
    'accuracy',
    'precision_pos',
    'recall_pos',
    'f1_pos',
    'directionalHitRate',
    'pred_pos_rate'
  ];

  const comparison = {};
  for (const field of fields) {
    const a = metricsA?.[field] ?? null;
    const b = metricsB?.[field] ?? null;
    const diff = (typeof a === 'number' && typeof b === 'number') ? a - b : null;

    comparison[field] = {
      a: a !== null ? Math.round(a * 10000) / 10000 : null,
      b: b !== null ? Math.round(b * 10000) / 10000 : null,
      diff: diff !== null ? Math.round(diff * 10000) / 10000 : null,
      better: diff === null ? 'unknown' : (diff > 0 ? 'a' : (diff < 0 ? 'b' : 'equal'))
    };
  }

  return comparison;
}

function compareDecisions(decisionA, decisionB) {
  if (!decisionA && !decisionB) return null;

  return {
    threshold: {
      a: decisionA?.best_threshold ?? null,
      b: decisionB?.best_threshold ?? null,
      diff: (decisionA?.best_threshold && decisionB?.best_threshold)
        ? Math.round((decisionA.best_threshold - decisionB.best_threshold) * 10000) / 10000
        : null
    },
    proba_source: {
      a: decisionA?.probaSource ?? null,
      b: decisionB?.probaSource ?? null,
      match: decisionA?.probaSource === decisionB?.probaSource
    },
    featureset: {
      a: decisionA?.featureset_version ?? decisionA?.featuresetVersion ?? null,
      b: decisionB?.featureset_version ?? decisionB?.featuresetVersion ?? null,
      match: (decisionA?.featureset_version ?? decisionA?.featuresetVersion) ===
             (decisionB?.featureset_version ?? decisionB?.featuresetVersion)
    },
    model_type: {
      a: decisionA?.model_type ?? null,
      b: decisionB?.model_type ?? null,
      match: decisionA?.model_type === decisionB?.model_type
    },
    symbol: {
      a: decisionA?.symbol ?? null,
      b: decisionB?.symbol ?? null,
      match: decisionA?.symbol === decisionB?.symbol
    }
  };
}

function generateRecommendation(metricsA, metricsB) {
  if (!metricsA || !metricsB) {
    return {
      winner: 'unknown',
      reason: 'Insufficient metrics data',
      confidence: 'low'
    };
  }

  const f1A = metricsA.f1_pos ?? 0;
  const f1B = metricsB.f1_pos ?? 0;
  const dirA = metricsA.directionalHitRate ?? 0;
  const dirB = metricsB.directionalHitRate ?? 0;
  const precA = metricsA.precision_pos ?? 0;
  const precB = metricsB.precision_pos ?? 0;

  // Weighted scoring: F1 (40%), Directional (40%), Precision (20%)
  const scoreA = f1A * 0.4 + dirA * 0.4 + precA * 0.2;
  const scoreB = f1B * 0.4 + dirB * 0.4 + precB * 0.2;

  const diff = Math.abs(scoreA - scoreB);
  let confidence = 'low';
  if (diff > 0.1) confidence = 'high';
  else if (diff > 0.05) confidence = 'medium';

  let winner, reason;
  if (scoreA > scoreB) {
    winner = 'model_a';
    reason = `Model A composite score: ${(scoreA * 100).toFixed(2)}% vs Model B: ${(scoreB * 100).toFixed(2)}%`;
  } else if (scoreB > scoreA) {
    winner = 'model_b';
    reason = `Model B composite score: ${(scoreB * 100).toFixed(2)}% vs Model A: ${(scoreA * 100).toFixed(2)}%`;
  } else {
    winner = 'tie';
    reason = 'Models have equivalent composite scores';
  }

  return {
    winner,
    reason,
    confidence,
    scores: {
      a: Math.round(scoreA * 10000) / 10000,
      b: Math.round(scoreB * 10000) / 10000
    },
    breakdown: {
      f1_weight: 0.4,
      directional_weight: 0.4,
      precision_weight: 0.2
    }
  };
}

function formatTextReport(report) {
  const lines = [];

  lines.push('='.repeat(60));
  lines.push('ML MODEL COMPARISON REPORT');
  lines.push('='.repeat(60));
  lines.push('');
  lines.push(`Model A: ${report.job_a}`);
  lines.push(`Model B: ${report.job_b}`);
  lines.push(`Generated: ${report.comparison_timestamp}`);
  lines.push('');

  // Decision Config Comparison
  lines.push('-'.repeat(60));
  lines.push('DECISION CONFIG COMPARISON');
  lines.push('-'.repeat(60));

  const config = report.config_comparison;
  if (config) {
    lines.push(`Threshold:     A=${config.threshold?.a ?? 'N/A'}  B=${config.threshold?.b ?? 'N/A'}  Diff=${config.threshold?.diff ?? 'N/A'}`);
    lines.push(`Proba Source:  A=${config.proba_source?.a ?? 'N/A'}  B=${config.proba_source?.b ?? 'N/A'}  Match=${config.proba_source?.match}`);
    lines.push(`Featureset:    A=${config.featureset?.a ?? 'N/A'}  B=${config.featureset?.b ?? 'N/A'}  Match=${config.featureset?.match}`);
    lines.push(`Model Type:    A=${config.model_type?.a ?? 'N/A'}  B=${config.model_type?.b ?? 'N/A'}  Match=${config.model_type?.match}`);
    lines.push(`Symbol:        A=${config.symbol?.a ?? 'N/A'}  B=${config.symbol?.b ?? 'N/A'}  Match=${config.symbol?.match}`);
  } else {
    lines.push('No decision config available');
  }
  lines.push('');

  // Metrics Comparison
  lines.push('-'.repeat(60));
  lines.push('METRICS COMPARISON');
  lines.push('-'.repeat(60));

  const metrics = report.metrics_comparison;
  if (metrics) {
    lines.push(String('Metric').padEnd(20) + String('Model A').padEnd(12) + String('Model B').padEnd(12) + String('Diff').padEnd(12) + 'Better');
    lines.push('-'.repeat(60));

    for (const [key, val] of Object.entries(metrics)) {
      const label = key.replace(/_/g, ' ');
      const aVal = val.a !== null ? val.a.toFixed(4) : 'N/A';
      const bVal = val.b !== null ? val.b.toFixed(4) : 'N/A';
      const diff = val.diff !== null ? (val.diff >= 0 ? '+' : '') + val.diff.toFixed(4) : 'N/A';
      const better = val.better === 'a' ? '← A' : (val.better === 'b' ? 'B →' : '=');

      lines.push(label.padEnd(20) + aVal.padEnd(12) + bVal.padEnd(12) + diff.padEnd(12) + better);
    }
  } else {
    lines.push('No metrics available');
  }
  lines.push('');

  // Recommendation
  lines.push('-'.repeat(60));
  lines.push('RECOMMENDATION');
  lines.push('-'.repeat(60));

  const rec = report.recommendation;
  lines.push(`Winner:     ${rec.winner.toUpperCase()}`);
  lines.push(`Confidence: ${rec.confidence}`);
  lines.push(`Reason:     ${rec.reason}`);
  if (rec.scores) {
    lines.push(`Scores:     A=${rec.scores.a}  B=${rec.scores.b}`);
  }
  lines.push('');
  lines.push('='.repeat(60));

  return lines.join('\n');
}

async function main() {
  const args = parseArgs(process.argv);

  if (args.help || (!args.job_a && !args.job_b)) {
    console.log(`
ML Model Comparison Tool

Usage:
  node tools/ml-compare.js --job-a <jobId> --job-b <jobId> [options]

Options:
  --job-a <id>     First ML job ID (required)
  --job-b <id>     Second ML job ID (required)
  --output <fmt>   Output format: json or text (default: text)
  --artifacts-dir  ML artifacts directory (default: core/ml/artifacts/jobs)
  --help           Show this help

Examples:
  node tools/ml-compare.js --job-a test-job-001 --job-b test-job-002
  node tools/ml-compare.js --job-a test-job-001 --job-b test-job-002 --output json
`);
    process.exit(0);
  }

  const jobA = args.job_a;
  const jobB = args.job_b;
  const outputFormat = args.output || 'text';
  const artifactsDir = args.artifacts_dir || process.env.ML_ARTIFACTS_DIR || DEFAULT_ARTIFACTS_DIR;

  if (!jobA || !jobB) {
    console.error('Error: Both --job-a and --job-b are required');
    process.exit(1);
  }

  if (jobA === jobB) {
    console.error('Error: Cannot compare a job with itself');
    process.exit(1);
  }

  // Load artifacts
  const artifactsA = await loadModelArtifacts(jobA, artifactsDir);
  const artifactsB = await loadModelArtifacts(jobB, artifactsDir);

  if (!artifactsA.exists) {
    console.error(`Error: ML job not found: ${jobA}`);
    console.error(`Looked in: ${path.join(artifactsDir, jobA)}`);
    process.exit(1);
  }

  if (!artifactsB.exists) {
    console.error(`Error: ML job not found: ${jobB}`);
    console.error(`Looked in: ${path.join(artifactsDir, jobB)}`);
    process.exit(1);
  }

  // Build comparison report
  const report = {
    job_a: jobA,
    job_b: jobB,
    comparison_timestamp: new Date().toISOString(),
    config_comparison: compareDecisions(artifactsA.decision, artifactsB.decision),
    metrics_comparison: compareMetrics(artifactsA.metrics, artifactsB.metrics),
    recommendation: generateRecommendation(artifactsA.metrics, artifactsB.metrics),
    artifacts: {
      a: {
        has_decision: !!artifactsA.decision,
        has_metrics: !!artifactsA.metrics,
        has_job: !!artifactsA.job
      },
      b: {
        has_decision: !!artifactsB.decision,
        has_metrics: !!artifactsB.metrics,
        has_job: !!artifactsB.job
      }
    }
  };

  // Output
  if (outputFormat === 'json') {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log(formatTextReport(report));
  }

  process.exit(0);
}

main().catch((err) => {
  console.error('Error:', err.message);
  process.exit(1);
});
