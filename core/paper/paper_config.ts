/**
 * PaperConfig interface for Paper Execution Adapter v1
 */
export interface PaperConfig {
    fill_probability: number;   // 0.0 to 1.0 (1.0 = 100% fill)
    avg_latency_ms: number;     // Fixed latency to add to execution time
    slippage_bps: number;       // Slippage in basis points (1 bps = 0.01%)
    price_placeholder: number;  // Fallback price if not present in input
}
