import { Position } from "./position";
import { PaperExecutionResult } from "../paper/paper_execution_result";

/**
 * Pure function to update a position based on a new execution result.
 * 
 * Logic:
 * - If position is increased (moving further long or further short), update avg_entry_price.
 * - If position is reduced (closing or partial close), calculate realized_pnl.
 * - If position flips (long to short or vice versa), handle partially as close and partially as new position.
 */
export function updatePosition(
    current: Position | undefined,
    exec: PaperExecutionResult
): Position {
    const pos: Position = current || {
        symbol: exec.symbol,
        quantity: 0,
        avg_entry_price: 0,
        realized_pnl: 0,
        unrealized_pnl: 0
    };

    const sideMultiplier = exec.side === "BUY" ? 1 : -1;
    const execQty = exec.filled_quantity * sideMultiplier;
    const execPrice = exec.fill_price;

    let newQty = pos.quantity + execQty;
    let newAvgEntryPrice = pos.avg_entry_price;
    let newRealizedPnl = pos.realized_pnl;

    const isIncreasing = (pos.quantity >= 0 && execQty > 0) || (pos.quantity <= 0 && execQty < 0);
    const isClosing = (pos.quantity > 0 && execQty < 0) || (pos.quantity < 0 && execQty > 0);

    if (isIncreasing) {
        // Updating average entry price
        const totalCost = Math.abs(pos.quantity) * pos.avg_entry_price + Math.abs(execQty) * execPrice;
        newAvgEntryPrice = totalCost / Math.abs(newQty);
    } else if (isClosing) {
        const closedQty = Math.min(Math.abs(pos.quantity), Math.abs(execQty));
        const pnlMultiplier = pos.quantity > 0 ? 1 : -1;
        const realized = closedQty * (execPrice - pos.avg_entry_price) * pnlMultiplier;
        newRealizedPnl += realized;

        if (Math.abs(execQty) > Math.abs(pos.quantity)) {
            // Position flipped
            const remainingQty = execQty + pos.quantity;
            newAvgEntryPrice = execPrice;
        } else if (newQty === 0) {
            newAvgEntryPrice = 0;
        }
    }

    return {
        ...pos,
        quantity: newQty,
        avg_entry_price: newAvgEntryPrice,
        realized_pnl: newRealizedPnl
    };
}

/**
 * Calculate unrealized PnL based on current market price.
 */
export function calculateUnrealized(pos: Position, currentPrice: number): number {
    if (pos.quantity === 0) return 0;
    const sideMultiplier = pos.quantity > 0 ? 1 : -1;
    return Math.abs(pos.quantity) * (currentPrice - pos.avg_entry_price) * sideMultiplier;
}
