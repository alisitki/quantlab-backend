#!/usr/bin/env node

/**
 * ML Feature Analysis CLI Tool
 *
 * Usage:
 *   node tools/ml-feature-analysis.js --help
 *   node tools/ml-feature-analysis.js --dataset ./data.parquet --analysis correlation
 *   node tools/ml-feature-analysis.js --dataset ./data.parquet --analysis full --output ./reports
 */

import { parseArgs } from 'node:util';
import fs from 'fs';
import path from 'path';

// Analysis modules
import { analyzeFeatureCorrelations, findHighlyCorrelatedPairs } from '../core/ml/analysis/FeatureCorrelation.js';
import { analyzeFeatureLabelRelationships } from '../core/ml/analysis/FeatureLabelCorrelation.js';
import { analyzeLabelDistribution } from '../core/ml/analysis/LabelDistribution.js';
import { generateFeatureDistributionReport } from '../core/ml/analysis/FeatureDistribution.js';
import { generateFeatureReport, formatReportAsMarkdown, formatReportAsJSON } from '../core/ml/analysis/FeatureReportGenerator.js';

const HELP = `
ML Feature Analysis Tool
========================

Analyze feature quality, correlations, and alpha potential.

Usage:
  node tools/ml-feature-analysis.js [options]

Options:
  --dataset <path>     Path to parquet file or JSON dataset
  --analysis <type>    Analysis type: correlation, label, distribution, full (default: full)
  --output <dir>       Output directory for reports (default: ./reports)
  --format <fmt>       Output format: json, markdown, both (default: both)
  --help               Show this help message

Analysis Types:
  correlation    Feature-to-feature correlation matrix
  label          Feature-to-label correlation and rankings
  distribution   Feature distribution statistics and outliers
  full           Complete analysis with alpha scores

Examples:
  # Quick correlation check
  node tools/ml-feature-analysis.js --dataset ./features.parquet --analysis correlation

  # Full report
  node tools/ml-feature-analysis.js --dataset ./features.parquet --analysis full --output ./reports

  # Label analysis only
  node tools/ml-feature-analysis.js --dataset ./features.parquet --analysis label
`;

async function main() {
  const { values } = parseArgs({
    options: {
      dataset: { type: 'string' },
      analysis: { type: 'string', default: 'full' },
      output: { type: 'string', default: './reports' },
      format: { type: 'string', default: 'both' },
      help: { type: 'boolean', default: false }
    }
  });

  if (values.help || !values.dataset) {
    console.log(HELP);
    process.exit(values.help ? 0 : 1);
  }

  const { dataset, analysis, output, format } = values;

  console.log('ML Feature Analysis Tool');
  console.log('========================\n');

  // Load dataset
  console.log(`Loading dataset: ${dataset}`);
  const data = await loadDataset(dataset);
  console.log(`Loaded ${data.X.length} samples, ${data.featureNames.length} features\n`);

  // Run analysis
  let result;

  switch (analysis) {
    case 'correlation':
      result = await runCorrelationAnalysis(data);
      break;
    case 'label':
      result = await runLabelAnalysis(data);
      break;
    case 'distribution':
      result = await runDistributionAnalysis(data);
      break;
    case 'full':
    default:
      result = await runFullAnalysis(data);
      break;
  }

  // Output results
  await outputResults(result, output, format, analysis);

  console.log('\nAnalysis complete!');
}

async function loadDataset(datasetPath) {
  const ext = path.extname(datasetPath).toLowerCase();

  if (ext === '.json') {
    const raw = fs.readFileSync(datasetPath, 'utf8');
    return JSON.parse(raw);
  }

  if (ext === '.parquet') {
    // Try to load parquet using existing infrastructure
    try {
      const { loadParquetData } = await import('../core/ml/dataset/parquetLoader.js');
      return await loadParquetData(datasetPath);
    } catch (err) {
      console.error('Parquet loading not available. Please provide JSON dataset.');
      console.error('Expected JSON format: { X: [[features]], y: [labels], featureNames: [names] }');
      process.exit(1);
    }
  }

  throw new Error(`Unsupported file format: ${ext}`);
}

async function runCorrelationAnalysis(data) {
  console.log('Running correlation analysis...\n');

  const result = analyzeFeatureCorrelations(data.X, data.featureNames);

  // Print summary
  console.log('Correlation Analysis Summary');
  console.log('----------------------------');
  console.log(`Total features: ${result.summary.totalFeatures}`);
  console.log(`High correlation pairs: ${result.summary.highCorrPairs}`);
  console.log(`Redundancy score: ${result.summary.redundancyScore}`);
  console.log(`Clusters found: ${result.summary.clustersFound}`);

  if (result.highlyCorrelatedPairs.length > 0) {
    console.log('\nHighly correlated pairs:');
    for (const pair of result.highlyCorrelatedPairs.slice(0, 5)) {
      console.log(`  ${pair.feature_a} <-> ${pair.feature_b}: ${pair.correlation.toFixed(3)}`);
    }
  }

  if (result.dropCandidates.length > 0) {
    console.log(`\nDrop candidates: ${result.dropCandidates.join(', ')}`);
  }

  return { type: 'correlation', ...result };
}

async function runLabelAnalysis(data) {
  console.log('Running label correlation analysis...\n');

  const result = analyzeFeatureLabelRelationships(data.X, data.y, data.featureNames);

  // Print summary
  console.log('Label Correlation Summary');
  console.log('-------------------------');
  console.log(`Binary label: ${result.isBinaryLabel ? 'Yes' : 'No'}`);
  console.log(`Avg absolute correlation: ${result.summary.avgAbsCorrelation}`);
  console.log(`Strong features: ${result.summary.strengthDistribution.STRONG}`);
  console.log(`Weak features: ${result.summary.weakFeatureCount}`);

  console.log('\nTop features by predictive power:');
  for (const r of result.rankings.slice(0, 5)) {
    console.log(`  ${r.rank}. ${r.feature}: ${r.correlation?.toFixed(4) || 'N/A'} (${r.strength})`);
  }

  console.log(`\nRecommendation: ${result.summary.recommendation}`);

  return { type: 'label', ...result };
}

async function runDistributionAnalysis(data) {
  console.log('Running distribution analysis...\n');

  const result = generateFeatureDistributionReport(data.X, data.featureNames);

  // Print summary
  console.log('Distribution Analysis Summary');
  console.log('-----------------------------');
  console.log(`Total features: ${result.summary.totalFeatures}`);
  console.log(`Features with alerts: ${result.summary.featuresWithAlerts}`);
  console.log(`High outlier features: ${result.summary.highOutlierCount}`);

  if (result.alerts.length > 0) {
    console.log('\nAlerts:');
    for (const alert of result.alerts.slice(0, 10)) {
      console.log(`  [${alert.type}] ${alert.feature}: ${alert.value}`);
    }
  }

  return { type: 'distribution', ...result };
}

async function runFullAnalysis(data) {
  console.log('Running full analysis...\n');

  const report = await generateFeatureReport({
    X: data.X,
    y: data.y,
    featureNames: data.featureNames,
    timestamps: data.timestamps || null
  });

  // Print summary
  console.log('Full Analysis Summary');
  console.log('---------------------');
  for (const [key, value] of Object.entries(report.summary)) {
    console.log(`${key}: ${value}`);
  }

  console.log('\nTop Alpha Features:');
  for (const r of report.alphaScores.ranked.slice(0, 5)) {
    console.log(`  ${r.rank}. ${r.feature}: ${r.score.toFixed(3)}`);
  }

  console.log('\nRecommendations:');
  for (const rec of report.recommendations) {
    console.log(`  - ${rec}`);
  }

  return report;
}

async function outputResults(result, outputDir, format, analysisType) {
  // Create output directory
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  const timestamp = new Date().toISOString().slice(0, 10);
  const baseName = `feature_analysis_${analysisType}_${timestamp}`;

  // JSON output
  if (format === 'json' || format === 'both') {
    const jsonPath = path.join(outputDir, `${baseName}.json`);
    fs.writeFileSync(jsonPath, JSON.stringify(result, null, 2));
    console.log(`\nJSON report saved: ${jsonPath}`);
  }

  // Markdown output (only for full analysis)
  if ((format === 'markdown' || format === 'both') && result.generatedAt) {
    const mdPath = path.join(outputDir, `${baseName}.md`);
    fs.writeFileSync(mdPath, formatReportAsMarkdown(result));
    console.log(`Markdown report saved: ${mdPath}`);
  }
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
