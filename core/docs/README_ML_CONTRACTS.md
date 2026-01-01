# QuantLab ML Contracts v1

This directory contains the deterministic dataset and feature contracts for QuantLab ML.

## 📌 Contracts List

1. **[MLDataset v1 Contract](MLDataset_v1.md)**
   - Main dataset definition, input/output schemas, and determinism rules.
2. **[FeatureSet v1 Specification](FeatureSet_v1.md)**
   - Definitions of 10 core features (Mid, Spread, Imbalance, Returns, Volatility).
3. **[Labeling v1 Specification](Labeling_v1.md)**
   - 10-second binary directional label definition and alignment rules.
4. **[Storage Layout Specification v1](StorageLayout_v1.md)**
   - S3 bucket mappings, Hive partitioning, and metadata standards.

## 🚀 Key Assumptions (v1)
- **Symbol**: `btcusdt`
- **Date**: `20251229` (Test/Deterministic Target)
- **Model**: XGBoost Binary Classifier
- **Label Horizon**: 10 seconds

## 🛠 Scheduler Integration
The ML Scheduler uses these contracts to generate `JobSpecs`. A compliant training job must consume data according to the schemas defined in `MLDataset_v1.md` and `StorageLayout_v1.md`.

## ⏭ v2 Roadmap (Multi-Horizon)
- Expansion to $\Delta \in \{5, 10, 30, 60\}$ seconds.
- Multi-label schema: `label_dir_5s`, `label_dir_10s`, etc.
- Promotion logic based on specific horizons.
 Linda v1’de multi-horizon pasif.
