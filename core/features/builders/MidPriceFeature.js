/**
 * MidPriceFeature: mid = (bid + ask) / 2
 */
export class MidPriceFeature {
  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);
    
    if (isNaN(bid) || isNaN(ask)) return null;
    
    return (bid + ask) / 2;
  }

  reset() {
    // Stateless
  }
}
