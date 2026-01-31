import { EventSource } from 'eventsource';

/**
 * ReplayStreamClient — Robust SSE client for replayd
 * 
 * Handles:
 * - Connection & Auth (Bearer/Token)
 * - Exponential backoff reconnections
 * - lastCursor tracking
 * - Stream lifecycle management
 */
export class ReplayStreamClient {
  #url;
  #token;
  #lastCursor;
  #eventSource = null;
  #onEvent;
  #onError;
  #onEnd;
  #running = false;
  #reconnectCount = 0;
  #maxRetries = 50;
  #backoffMs = 1000;
  #connResolve = null;
  #timeoutTimer = null;
  #firstEventReceived = false;
  #connectTime = 0;

  // New Metrics (exposed for SSEStrategyRunner)
  metrics = {
    connectsTotal: 0,
    timeoutsTotal: 0,
    firstEventLatencyMs: 0
  };

  /**
   * @param {Object} opts
   * @param {string} opts.url - Base URL of replayd/stream
   * @param {string} [opts.token] - Auth token
   * @param {string} [opts.cursor] - Initial cursor
   * @param {Function} opts.onEvent - Callback for data events
   * @param {Function} [opts.onError] - Callback for errors
   * @param {Function} [opts.onEnd] - Callback for stream end
   */
  constructor(opts) {
    this.#url = opts.url;
    this.#token = opts.token;
    this.#lastCursor = opts.cursor || null;
    this.#onEvent = opts.onEvent;
    this.#onError = opts.onError || console.error;
    this.#onEnd = opts.onEnd || (() => {});
  }

  get lastCursor() { return this.#lastCursor; }
  get reconnectCount() { return this.#reconnectCount; }
  get running() { return this.#running; }

  /**
   * Start the client
   */
  async start() {
    if (this.#running) return;
    this.#running = true;
    this.#reconnectCount = 0;
    this.#backoffMs = 1000;

    const urlObj = new URL(this.#url);
    const params = Object.fromEntries(urlObj.searchParams);
    console.log(`[ReplayStreamClient] STARTING dataset=${params.dataset} symbol=${params.symbol} date=${params.date} cursor=${this.#lastCursor} auth=${this.#token ? 'Bearer' : 'None'}`);

    while (this.#running && this.#reconnectCount < this.#maxRetries) {
      try {
        await this.#connect();
        // If #connect resolves normally, stream ended
        this.#running = false;
        this.#onEnd();
        break;
      } catch (err) {
        if (!this.#running) break;
        
        if (err.message === 'EMPTY_STREAM') {
          console.log('[ReplayStreamClient] TERMINAL: EMPTY_STREAM received. Stopping.');
          this.#running = false;
          this.#onEnd();
          break;
        }

        this.#reconnectCount++;
        console.log(`[ReplayStreamClient] STREAM_RECONNECTING count=${this.#reconnectCount} backoff=${this.#backoffMs}ms reason=${err.message}`);
        this.#onError(err);
        
        await new Promise(r => setTimeout(r, this.#backoffMs));
        this.#backoffMs = Math.min(this.#backoffMs * 2, 30000);
      }
    }

    if (this.#reconnectCount >= this.#maxRetries) {
      this.#running = false;
      this.#onError(new Error('MAX_RECONNECT_RETRIES_EXCEEDED'));
    }
  }

  /**
   * Stop the client
   */
  stop() {
    this.#running = false;
    this.#disconnect();
    if (this.#connResolve) {
      this.#connResolve();
      this.#connResolve = null;
    }
  }

  #startTimeout(ms, isFirst) {
    this.#clearTimeout();
    this.#timeoutTimer = setTimeout(() => {
      console.warn(`[ReplayStreamClient] STREAM_TIMEOUT (${isFirst ? 'first event' : 'inactivity'}) after ${ms}ms`);
      this.metrics.timeoutsTotal++;
      this.#disconnect();
      if (this.#connResolve) {
        const resolve = this.#connResolve;
        this.#connResolve = null;
        resolve(new Error('STREAM_TIMEOUT')); // We want start() loop to catch this and reconnect
      }
    }, ms);
  }

  #clearTimeout() {
    if (this.#timeoutTimer) {
      clearTimeout(this.#timeoutTimer);
      this.#timeoutTimer = null;
    }
  }

  #connect() {
    return new Promise((resolve, reject) => {
      this.#connResolve = (err) => {
        this.#clearTimeout();
        if (err) reject(err);
        else resolve();
      };

      console.log('[ReplayStreamClient] STREAM_CONNECTING');
      this.metrics.connectsTotal++;
      this.#firstEventReceived = false;
      this.#connectTime = Date.now();

      const urlObj = new URL(this.#url);
      if (this.#token) urlObj.searchParams.set('token', this.#token);
      if (this.#lastCursor) urlObj.searchParams.set('cursor', this.#lastCursor);

      const headers = {};
      if (this.#token) headers['Authorization'] = `Bearer ${this.#token}`;

      this.#eventSource = new EventSource(urlObj.toString(), { headers });

      // Start initial 5s timeout
      this.#startTimeout(5000, true);

      this.#eventSource.onopen = () => {
        console.log('[ReplayStreamClient] STREAM_CONNECTED');
      };

      this.#eventSource.onmessage = (msg) => {
        if (!this.#running) return;
        
        // Reset/Restart inactivity timeout (30s)
        this.#startTimeout(30000, false);

        try {
          const data = JSON.parse(msg.data);
          
          if (!this.#firstEventReceived) {
            this.#firstEventReceived = true;
            const latency = Date.now() - this.#connectTime;
            this.metrics.firstEventLatencyMs = latency;
            console.log(`[ReplayStreamClient] FIRST_EVENT_RECEIVED latency=${latency}ms first_cursor=${data.cursor}`);
          }

          if (data.cursor) this.#lastCursor = data.cursor;
          
          if (data.error === 'EMPTY_STREAM') {
            this.#disconnect();
            this.#connResolve(new Error('EMPTY_STREAM'));
            return;
          }

          this.#onEvent(data);
        } catch (err) {
          console.error(`[ReplayStreamClient] STREAM_ERROR parse_fail=${err.message}`);
          this.#onError(new Error(`PARSE_ERROR: ${err.message}`));
        }
      };

      this.#eventSource.onerror = (err) => {
        if (!this.#running) {
          this.#connResolve(); 
          return;
        }
        // EventSource.CLOSED is the only "normal" exit for our replayd implementation
        if (this.#eventSource?.readyState === 2) { // 2 = CLOSED
          this.#connResolve();
        } else {
          this.#disconnect();
          this.#connResolve(new Error('SSE_CONNECTION_FAILED'));
        }
      };

      this.#eventSource.addEventListener('error', (e) => {
        if (e.data) {
          const errData = JSON.parse(e.data);
          this.#disconnect();
          this.#connResolve(new Error(errData.error || 'SERVER_ERROR'));
        }
      });
    });
  }

  #disconnect() {
    this.#clearTimeout();
    if (this.#eventSource) {
      this.#eventSource.close();
      this.#eventSource = null;
    }
  }
}
