/**
 * Position interface for Paper Portfolio Engine v1
 */
export interface Position {
    symbol: string;
    quantity: number;           // Net position (+ long, - short)
    avg_entry_price: number;
    realized_pnl: number;
    unrealized_pnl: number;
}
