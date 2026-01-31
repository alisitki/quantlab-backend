/**
 * MLRuntime: Orchestrates job execution using backends.
 */
import fs from 'fs';
import path from 'path';
import parquet from 'parquetjs-lite';
import { CpuBackend } from './backends/CpuBackend.js';
import { GpuBackend } from './backends/GpuBackend.js';

export class MLRuntime {
  /**
   * Run a training job according to its JobSpec.
   * @param {JobSpec} jobSpec 
   */
  static async run(jobSpec) {
    const startTime = Date.now();
    const backendType = jobSpec.runtime.backend;
    
    // 1. Select Backend
    let backend;
    if (backendType === 'gpu') {
      backend = new GpuBackend();
    } else {
      backend = new CpuBackend();
    }

    try {
      if (jobSpec.seed !== undefined && jobSpec.model && jobSpec.model.params && jobSpec.model.params.seed === undefined) {
        jobSpec.model.params.seed = jobSpec.seed;
      }
      // 0. Prepare dataset bindings (features + labels)
      await prepareMergedDataset(jobSpec);

      // 2. Prepare
      await backend.prepare(jobSpec);

      // 3. Execute
      const result = await backend.run(jobSpec);

      // 4. Persistence of Meta Artifacts
      const endTime = Date.now();
      const runtimeInfo = {
        backend: backendType,
        hostname: process.env.HOSTNAME || 'localhost',
        startTimestamp: new Date(startTime).toISOString(),
        endTimestamp: new Date(endTime).toISOString(),
        durationMs: endTime - startTime
      };

      const outDir = path.dirname(jobSpec.output.artifactPath);
      fs.writeFileSync(path.join(outDir, 'job.json'), JSON.stringify(jobSpec.toJSON(), null, 2));
      fs.writeFileSync(path.join(outDir, 'runtime.json'), JSON.stringify(runtimeInfo, null, 2));
      await writeDecisionArtifact(jobSpec, result.metrics, outDir);

      console.log(`[MLRuntime] Job ${jobSpec.jobId} completed in ${(endTime - startTime) / 1000}s`);
      
      return {
        jobId: jobSpec.jobId,
        backendUsed: backendType,
        artifactDir: outDir,
        metrics: result.metrics
      };
    } catch (err) {
      console.error(`[MLRuntime] Job ${jobSpec.jobId} failed:`, err);
      throw err;
    } finally {
      // 5. Cleanup
      await backend.cleanup(jobSpec);
    }
  }
}

async function prepareMergedDataset(jobSpec) {
  const dataset = jobSpec.dataset || {};
  if (!dataset.featurePath || !dataset.labelPath) return;

  const outDir = path.dirname(jobSpec.output.artifactPath);
  const mergedPath = path.join(outDir, 'dataset.parquet');
  dataset.mergedPath = mergedPath;

  const featureManifest = await readJson(path.join(path.dirname(dataset.featurePath), 'dataset_manifest.json'));
  const labelManifest = await readJson(path.join(path.dirname(dataset.labelPath), 'label_manifest.json'));

  const featureColumns = featureManifest?.features || null;

  const featureReader = await parquet.ParquetReader.openFile(dataset.featurePath);
  const labelReader = await parquet.ParquetReader.openFile(dataset.labelPath);
  const featureCursor = featureReader.getCursor();
  const labelCursor = labelReader.getCursor();

  let featureRow = await featureCursor.next();
  let labelRow = await labelCursor.next();

  if (!featureRow || !labelRow) {
    await featureReader.close();
    await labelReader.close();
    throw new Error('DATASET_EMPTY');
  }

  const featureList = featureColumns || Object.keys(featureRow).filter((k) => k.startsWith('f_')).sort();

  const schema = buildMergedSchema(featureList);
  const writer = await parquet.ParquetWriter.openFile(schema, mergedPath);

  while (featureRow && labelRow) {
    const tsEvent = toBigInt(featureRow.ts_event);
    const seq = toBigInt(featureRow.seq ?? 0);
    const labelTs = toBigInt(labelRow.ts_event);
    const labelSeq = toBigInt(labelRow.seq ?? 0);

    if (tsEvent !== labelTs || seq !== labelSeq) {
      await writer.close();
      await featureReader.close();
      await labelReader.close();
      throw new Error(`ORDERING_MISMATCH: ${tsEvent}:${seq} vs ${labelTs}:${labelSeq}`);
    }

    const row = {
      ts_event: tsEvent,
      seq,
      label_dir_10s: labelRow.label_direction ?? null
    };

    for (const name of featureList) {
      row[name] = Number(featureRow[name]);
    }

    await writer.appendRow(row);

    featureRow = await featureCursor.next();
    labelRow = await labelCursor.next();
  }

  if (featureRow || labelRow) {
    await writer.close();
    await featureReader.close();
    await labelReader.close();
    throw new Error('ROW_COUNT_MISMATCH');
  }

  await writer.close();
  await featureReader.close();
  await labelReader.close();

  dataset.featurePath = mergedPath;
  dataset.feature_manifest = featureManifest;
  dataset.label_manifest = labelManifest;
}

function buildMergedSchema(featureList) {
  const fields = {
    ts_event: { type: 'INT64' },
    seq: { type: 'INT64' },
    label_dir_10s: { type: 'INT32', optional: true }
  };

  for (const name of featureList) {
    fields[name] = { type: 'DOUBLE' };
  }

  return new parquet.ParquetSchema(fields);
}

async function writeDecisionArtifact(jobSpec, metrics, outDir) {
  if (!metrics) return;

  const featureManifest = jobSpec.dataset?.feature_manifest || await readJson(path.join(path.dirname(jobSpec.dataset?.featurePath || ''), 'dataset_manifest.json'));
  const labelManifest = jobSpec.dataset?.label_manifest || await readJson(path.join(path.dirname(jobSpec.dataset?.labelPath || ''), 'label_manifest.json'));

  const thresholds = metrics.threshold_results ? Object.keys(metrics.threshold_results).map((v) => Number(v)) : [0.4, 0.45, 0.5, 0.55, 0.6];
  thresholds.sort((a, b) => a - b);

  const best = metrics.best_threshold?.value ?? 0.55;
  const generatedAt = deriveGeneratedAt(featureManifest?.date_range);

  const decision = {
    schema_version: 'v1',
    model_type: jobSpec.model?.type || 'xgboost_v1',
    model_version: jobSpec.model?.version || 'v1',
    featureset_id: featureManifest?.feature_set_id || null,
    featureset_version: featureManifest?.feature_set_version || null,
    label_set_id: labelManifest?.label_set_id || null,
    label_set_version: labelManifest?.label_set_version || null,
    best_threshold: best,
    bestThreshold: best,
    threshold_grid: thresholds,
    thresholdGrid: thresholds,
    metric: 'accuracy',
    primaryMetric: 'accuracy',
    generated_at: generatedAt,
    symbol: deriveSymbol(featureManifest?.symbols),
    probaSource: metrics.proba_source || 'unknown',
    labelHorizonSec: labelManifest?.label_horizon_sec || null,
    featuresetVersion: featureManifest?.feature_set_version || null
  };

  fs.writeFileSync(path.join(outDir, 'decision.json'), JSON.stringify(decision, null, 2));
}

function deriveGeneratedAt(dateRange) {
  if (Array.isArray(dateRange) && dateRange[1]) {
    return `${dateRange[1]}T00:00:00.000Z`;
  }
  return '1970-01-01T00:00:00.000Z';
}

function deriveSymbol(symbols) {
  if (Array.isArray(symbols) && symbols.length === 1) return symbols[0];
  return null;
}

async function readJson(filePath) {
  if (!filePath) return null;
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function toBigInt(value) {
  if (typeof value === 'bigint') return value;
  if (typeof value === 'number' && Number.isFinite(value)) return BigInt(Math.trunc(value));
  if (typeof value === 'string' && value.trim() !== '') {
    try {
      return BigInt(value.trim());
    } catch {
      return BigInt(0);
    }
  }
  return BigInt(0);
}
