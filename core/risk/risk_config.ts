/**
 * RiskConfig interface for Position Sizing Engine v1
 */
export interface RiskConfig {
    max_risk_pct_per_trade: number;   // e.g. 0.01 (1%)
    max_notional_usd: number;         // Max USD size for a single order
    assumed_stop_pct: number;         // e.g. 0.005 (0.5%) for risk calculation

    // Added for stateless support: base capital to calculate risk-based size
    reference_capital?: number;       // e.g. 10000
}
