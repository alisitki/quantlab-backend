/**
 * SpreadFeature: spread = ask - bid
 */
export class SpreadFeature {
  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);
    
    if (isNaN(bid) || isNaN(ask)) return null;
    
    return ask - bid;
  }

  reset() {
    // Stateless
  }
}
