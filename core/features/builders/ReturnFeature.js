/**
 * ReturnFeature: return_1 = (mid - prev_mid) / prev_mid
 */
export class ReturnFeature {
  #prevMid = null;

  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);
    
    if (isNaN(bid) || isNaN(ask)) return null;
    
    const mid = (bid + ask) / 2;
    
    let ret = null;
    if (this.#prevMid !== null && this.#prevMid > 0) {
      ret = (mid - this.#prevMid) / this.#prevMid;
    }
    
    this.#prevMid = mid;
    return ret;
  }

  reset() {
    this.#prevMid = null;
  }
}
