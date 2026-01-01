/**
 * LabelBuilder: Generates deterministic labels for training.
 */
export class LabelBuilder {
  /**
   * Next-return sign: sign(mid_price[t+1] - mid_price[t])
   * 1 -> up, 0 -> flat/ignore, -1 -> down
   * 
   * @param {Array<Object>} features - Array of feature vectors
   * @returns {Array<number>} Labels
   */
  static buildNextReturnSign(features) {
    if (features.length < 2) return [];

    const labels = [];
    // We iterate up to length - 1 because we need index i + 1
    for (let i = 0; i < features.length - 1; i++) {
        const currentMid = features[i].mid_price;
        const nextMid = features[i + 1].mid_price;

        if (nextMid > currentMid) {
            labels.push(1);
        } else if (nextMid < currentMid) {
            labels.push(-1);
        } else {
            labels.push(0);
        }
    }

    return labels;
  }
}
