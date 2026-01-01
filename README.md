# QuantLab

High-performance trading infrastructure.

## Multi-Server Deployment

### 1. API (16GB VPS)
- **Components**: Compact, Replay, Strategy
- **Path**: `api/`
- **Usage**: Handles data processing and strategy execution.

### 2. Collector (8GB VPS)
- **Components**: Raw Data Ingestion
- **Path**: `collector/`
- **Usage**: Collects and stores raw events.

## Development Workflow
- **Development**: Both modules are in this same repository (Monorepo).
- **Commits**: Use prefixes like `api:` or `collector:`.
- **Deployment**: Each VPS pulls the entire repo but only runs its respective module.
