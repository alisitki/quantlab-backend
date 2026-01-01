/**
 * CpuBackend: Local execution on CPU.
 */
import fs from 'fs';
import path from 'path';
// import { ReplayEngine } from '../../../replay/ReplayEngine.js';
// import { FeatureRegistry } from '../../../features/FeatureRegistry.js';
import { DatasetBuilder } from '../../dataset/DatasetBuilder.js';
import { splitDataset } from '../../dataset/splits.js';
import { trainModel } from '../../train/train.js';
import { evaluateModel, evaluateThresholdGrid } from '../../train/evaluate.js';
import { XGBoostModel } from '../../models/XGBoostModel.js';
import { DummyBaselineModel } from '../../models/DummyBaselineModel.js';

export class CpuBackend {
  /**
   * Prepare environment (ensure directories exist)
   */
  async prepare(jobSpec) {
    const dir = path.dirname(jobSpec.output.artifactPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
  }

  /**
   * Run training job
   */
  async run(jobSpec) {
    console.log(`[CpuBackend] Running Job ${jobSpec.jobId}...`);

    // 2. Build Dataset from pre-calculated features
    const datasetBuilder = new DatasetBuilder();
    const ds = await datasetBuilder.loadFromParquet(jobSpec.dataset.featurePath);

    const split = splitDataset(ds.X, ds.y);

    // 3. Initialize Model
    let model;
    if (jobSpec.model.type === 'xgboost') {
      model = new XGBoostModel(jobSpec.model.params);
    } else if (jobSpec.model.type === 'dummy') {
      model = new DummyBaselineModel();
    } else {
      throw new Error(`Unsupported model type: ${jobSpec.model.type}`);
    }

    // 4. Train
    const trainResult = await trainModel(model, split.train, split.valid);

    // 5. Evaluate with threshold grid
    const thresholds = [0.50, 0.55, 0.60, 0.65, 0.70];
    const metrics = evaluateThresholdGrid(model, split.test, thresholds);
    
    // 6. Persist Artifacts
    await model.save(jobSpec.output.artifactPath);
    fs.writeFileSync(jobSpec.output.metricsPath, JSON.stringify(metrics, null, 2));

    return { metrics, trainResult };
  }

  /**
   * Cleanup resources
   */
  async cleanup(jobSpec) {
    // No-op for CPU backend
  }
}
