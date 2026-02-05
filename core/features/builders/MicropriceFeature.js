/**
 * MicropriceFeature: Imbalance-weighted mid price
 *
 * Formula: microprice = (bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty)
 * Stateless - no warm-up needed
 */
export class MicropriceFeature {
  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);
    const bidQty = Number(event.bid_qty ?? event.bid_size ?? 0);
    const askQty = Number(event.ask_qty ?? event.ask_size ?? 0);

    if (isNaN(bid) || isNaN(ask)) return null;

    const totalQty = bidQty + askQty;
    if (totalQty === 0) {
      // Fallback to simple mid if no quantity data
      return (bid + ask) / 2;
    }

    // Microprice: weighted towards where more quantity sits
    // If more bid_qty, price leans towards ask (upward pressure)
    return (bid * askQty + ask * bidQty) / totalQty;
  }

  reset() {
    // Stateless - nothing to reset
  }
}
