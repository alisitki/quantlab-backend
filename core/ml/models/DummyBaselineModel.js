/**
 * DummyBaselineModel: Predicts constant 0.
 */
export class DummyBaselineModel {
  #name = 'DummyBaseline';

  constructor() {}

  /**
   * Train: No-op for dummy model
   */
  async train(X, y) {
    console.log(`[${this.#name}] Training... (No-op)`);
  }

  /**
   * Predict: Always return 0
   * @param {Array<Array<number>>} X
   * @returns {Array<number>}
   */
  predict(X) {
    return new Array(X.length).fill(0);
  }

  /**
   * Save/Load: No-op
   */
  async save(path) {}
  async load(path) {}
}
