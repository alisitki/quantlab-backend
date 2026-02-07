/**
 * RegimeCluster: K-means clustering for regime detection
 *
 * Purpose: Discover regimes from continuous feature data using unsupervised learning.
 *
 * Input: Continuous regime features (volatility_ratio, trend_strength, spread_ratio)
 *        + behavior features (liquidity_pressure, return_momentum, etc.)
 *
 * Output: Cluster labels (regime IDs) + cluster centroids
 *
 * This is an OFFLINE training process. Centroids are saved and used for live prediction.
 */
export class RegimeCluster {
  #K; // Number of clusters
  #maxIterations;
  #tolerance;
  #centroids = null;
  #clusterStats = null;
  #featureNames = [];
  #trained = false;
  #seededRandom; // Seeded random function

  /**
   * @param {Object} config
   * @param {number} config.K - Number of clusters (default: 4)
   * @param {number} config.maxIterations - Max iterations (default: 100)
   * @param {number} config.tolerance - Convergence tolerance (default: 1e-4)
   * @param {number} config.seed - Random seed for reproducibility (default: 42)
   */
  constructor(config = {}) {
    this.#K = config.K || 4;
    this.#maxIterations = config.maxIterations || 100;
    this.#tolerance = config.tolerance || 1e-4;
    this.seed = config.seed || 42;
    this.#seededRandom = this.#createSeededRandom(this.seed);
  }

  /**
   * Create seeded random number generator for reproducibility
   */
  #createSeededRandom(seed) {
    let state = seed;
    return () => {
      state = (state * 1664525 + 1013904223) % 4294967296;
      return state / 4294967296;
    };
  }

  /**
   * Train K-means on feature data
   * @param {Array<Object>} data - Array of feature vectors
   * @param {Array<string>} featureNames - Feature names to use for clustering
   * @returns {Object} Training results
   */
  train(data, featureNames) {
    if (!data || data.length === 0) {
      throw new Error('Training data cannot be empty');
    }

    if (!featureNames || featureNames.length === 0) {
      throw new Error('Feature names must be specified');
    }

    this.#featureNames = featureNames;

    // Extract feature matrix
    const X = this.#extractFeatures(data, featureNames);

    if (X.length < this.#K) {
      throw new Error(`Not enough data points (${X.length}) for ${this.#K} clusters`);
    }

    // Normalize features
    const { normalized, means, stds } = this.#normalize(X);

    // Initialize centroids (K-means++)
    let centroids = this.#initializeCentroidsKMeansPlusPlus(normalized, this.#K);

    let iteration = 0;
    let converged = false;
    let labels = null;

    while (iteration < this.#maxIterations && !converged) {
      // Assign points to nearest centroid
      labels = this.#assignClusters(normalized, centroids);

      // Update centroids
      const newCentroids = this.#updateCentroids(normalized, labels, this.#K);

      // Check convergence
      const shift = this.#calculateCentroidShift(centroids, newCentroids);
      converged = shift < this.#tolerance;

      centroids = newCentroids;
      iteration++;
    }

    // Store centroids (denormalized)
    this.#centroids = centroids.map(c => this.#denormalize(c, means, stds));

    // Calculate cluster statistics
    this.#clusterStats = this.#calculateClusterStats(X, labels);

    this.#trained = true;

    return {
      iterations: iteration,
      converged,
      inertia: this.#calculateInertia(normalized, centroids, labels),
      clusterSizes: this.#clusterStats.sizes
    };
  }

  /**
   * Predict cluster for new feature vector
   * @param {Object} features - Feature vector
   * @returns {Object} { cluster: number, distance: number, confidence: number }
   */
  predict(features) {
    if (!this.#trained) {
      throw new Error('Model not trained. Call train() first.');
    }

    // Extract feature values (support both array and object formats)
    let x;
    if (Array.isArray(features)) {
      // Array format: use directly (memory optimization)
      x = features;
    } else {
      // Object format: extract by feature names (legacy)
      x = this.#featureNames.map(name => features[name]);
    }

    // Check for missing features
    if (x.some(v => v === null || v === undefined || isNaN(v))) {
      return { cluster: null, distance: null, confidence: 0 };
    }

    // Find nearest centroid
    let minDistance = Infinity;
    let nearestCluster = 0;

    for (let k = 0; k < this.#K; k++) {
      const distance = this.#euclideanDistance(x, this.#centroids[k]);
      if (distance < minDistance) {
        minDistance = distance;
        nearestCluster = k;
      }
    }

    // Calculate confidence (inverse of normalized distance)
    // Closer to centroid = higher confidence
    const maxDist = this.#clusterStats.maxDistances[nearestCluster] || 1;
    const confidence = Math.max(0, 1 - (minDistance / maxDist));

    return {
      cluster: nearestCluster,
      distance: minDistance,
      confidence
    };
  }

  /**
   * Extract feature matrix from data
   */
  #extractFeatures(data, featureNames) {
    return data.map((row, rowIdx) => {
      // If row is already an array, use it directly (memory optimization)
      if (Array.isArray(row)) {
        if (row.length !== featureNames.length) {
          throw new Error(`Array length mismatch at row ${rowIdx}: expected ${featureNames.length}, got ${row.length}`);
        }
        // Validate values
        for (let i = 0; i < row.length; i++) {
          if (row[i] === null || row[i] === undefined || isNaN(row[i])) {
            throw new Error(`Invalid feature value at row ${rowIdx}, index ${i}: ${row[i]}`);
          }
        }
        return row;
      }

      // Otherwise extract from object (legacy path)
      return featureNames.map(name => {
        const val = row[name];
        if (val === null || val === undefined || isNaN(val)) {
          console.error(`[RegimeCluster] Invalid value at row ${rowIdx}, feature ${name}: ${val}`);
          console.error(`[RegimeCluster] Row data: ${JSON.stringify(Object.keys(row))}`);
          throw new Error(`Invalid feature value for ${name} at row ${rowIdx}: ${val}`);
        }
        return val;
      });
    });
  }

  /**
   * Normalize features (z-score normalization)
   */
  #normalize(X) {
    const n = X.length;
    const d = X[0].length;

    // Calculate means
    const means = new Array(d).fill(0);
    for (let i = 0; i < n; i++) {
      for (let j = 0; j < d; j++) {
        means[j] += X[i][j];
      }
    }
    for (let j = 0; j < d; j++) {
      means[j] /= n;
    }

    // Calculate standard deviations
    const stds = new Array(d).fill(0);
    for (let i = 0; i < n; i++) {
      for (let j = 0; j < d; j++) {
        const diff = X[i][j] - means[j];
        stds[j] += diff * diff;
      }
    }
    for (let j = 0; j < d; j++) {
      stds[j] = Math.sqrt(stds[j] / n);
      if (stds[j] === 0) stds[j] = 1; // Avoid division by zero
    }

    // Normalize
    const normalized = X.map(x => {
      return x.map((val, j) => (val - means[j]) / stds[j]);
    });

    return { normalized, means, stds };
  }

  /**
   * Denormalize vector
   */
  #denormalize(x, means, stds) {
    return x.map((val, j) => val * stds[j] + means[j]);
  }

  /**
   * Initialize centroids using K-means++
   */
  #initializeCentroidsKMeansPlusPlus(X, K) {
    const n = X.length;
    const centroids = [];

    // Choose first centroid randomly
    const firstIdx = Math.floor(this.#seededRandom() * n);
    centroids.push([...X[firstIdx]]);

    // Choose remaining centroids
    for (let k = 1; k < K; k++) {
      // Calculate distance from each point to nearest centroid
      const distances = X.map(x => {
        let minDist = Infinity;
        for (const c of centroids) {
          const dist = this.#euclideanDistance(x, c);
          if (dist < minDist) minDist = dist;
        }
        return minDist * minDist; // Squared distance
      });

      // Choose next centroid with probability proportional to distance
      const totalDist = distances.reduce((sum, d) => sum + d, 0);
      let threshold = this.#seededRandom() * totalDist;
      let chosenIdx = 0;

      for (let i = 0; i < n; i++) {
        threshold -= distances[i];
        if (threshold <= 0) {
          chosenIdx = i;
          break;
        }
      }

      centroids.push([...X[chosenIdx]]);
    }

    return centroids;
  }

  /**
   * Assign each point to nearest centroid
   */
  #assignClusters(X, centroids) {
    return X.map(x => {
      let minDistance = Infinity;
      let nearestCluster = 0;

      for (let k = 0; k < centroids.length; k++) {
        const distance = this.#euclideanDistance(x, centroids[k]);
        if (distance < minDistance) {
          minDistance = distance;
          nearestCluster = k;
        }
      }

      return nearestCluster;
    });
  }

  /**
   * Update centroids as mean of assigned points
   */
  #updateCentroids(X, labels, K) {
    const d = X[0].length;
    const centroids = Array.from({ length: K }, () => new Array(d).fill(0));
    const counts = new Array(K).fill(0);

    // Sum points in each cluster
    for (let i = 0; i < X.length; i++) {
      const cluster = labels[i];
      counts[cluster]++;
      for (let j = 0; j < d; j++) {
        centroids[cluster][j] += X[i][j];
      }
    }

    // Calculate means
    for (let k = 0; k < K; k++) {
      if (counts[k] > 0) {
        for (let j = 0; j < d; j++) {
          centroids[k][j] /= counts[k];
        }
      }
      // If cluster is empty, reinitialize randomly
      else {
        const randomIdx = Math.floor(this.#seededRandom() * X.length);
        centroids[k] = [...X[randomIdx]];
      }
    }

    return centroids;
  }

  /**
   * Calculate centroid shift (for convergence check)
   */
  #calculateCentroidShift(oldCentroids, newCentroids) {
    let totalShift = 0;
    for (let k = 0; k < oldCentroids.length; k++) {
      totalShift += this.#euclideanDistance(oldCentroids[k], newCentroids[k]);
    }
    return totalShift / oldCentroids.length;
  }

  /**
   * Calculate inertia (sum of squared distances to centroids)
   */
  #calculateInertia(X, centroids, labels) {
    let inertia = 0;
    for (let i = 0; i < X.length; i++) {
      const cluster = labels[i];
      const dist = this.#euclideanDistance(X[i], centroids[cluster]);
      inertia += dist * dist;
    }
    return inertia;
  }

  /**
   * Calculate cluster statistics
   */
  #calculateClusterStats(X, labels) {
    const K = this.#centroids.length;
    const sizes = new Array(K).fill(0);
    const maxDistances = new Array(K).fill(0);

    for (let i = 0; i < X.length; i++) {
      const cluster = labels[i];
      sizes[cluster]++;

      const dist = this.#euclideanDistance(X[i], this.#centroids[cluster]);
      if (dist > maxDistances[cluster]) {
        maxDistances[cluster] = dist;
      }
    }

    return { sizes, maxDistances };
  }

  /**
   * Euclidean distance between two vectors
   */
  #euclideanDistance(a, b) {
    let sum = 0;
    for (let i = 0; i < a.length; i++) {
      const diff = a[i] - b[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }

  /**
   * Save model to JSON
   */
  toJSON() {
    if (!this.#trained) {
      throw new Error('Cannot serialize untrained model');
    }

    return {
      K: this.#K,
      featureNames: this.#featureNames,
      centroids: this.#centroids,
      clusterStats: this.#clusterStats,
      seed: this.seed,
      trained: this.#trained
    };
  }

  /**
   * Load model from JSON
   */
  static fromJSON(json) {
    const model = new RegimeCluster({
      K: json.K,
      seed: json.seed
    });

    model.#featureNames = json.featureNames;
    model.#centroids = json.centroids;
    model.#clusterStats = json.clusterStats;
    model.#trained = json.trained;

    return model;
  }

  /**
   * Get model info
   */
  getInfo() {
    return {
      trained: this.#trained,
      K: this.#K,
      featureCount: this.#featureNames.length,
      features: this.#featureNames,
      clusterSizes: this.#clusterStats?.sizes || null
    };
  }
}
