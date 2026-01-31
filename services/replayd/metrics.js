/**
 * Replayd Metrics Singleton
 */

export const replayMetrics = {
  connectionsActive: 0,
  eventsSentTotal: 0,
  bytesSentTotal: 0,
  streamErrorsTotal: 0,
  backpressureWaitsTotal: 0,
  clientTooSlowDisconnectsTotal: 0, // RENAMED
  cacheHitsTotal: 0,
  s3GetOpsTotal: 0,
  streamRequestsTotal: 0,
  streamEmptyTotal: 0,

  replayStreamEventsTotal: 0,
  replayStreamLastEventTs: 0,
  replayStreamReconnectTotal: 0,
  replayQueueDepth: 0,

  replayEngineCyclesTotal: 0,
  replayBackpressureTotal: 0,
  replayStreamRestartsTotal: 0,
  replayStreamErrorsTotal: 0,

  replayEventLoopLagMs: 0,
  replayActiveStreams: 0,
  replayProcessingLatencyMs: 0,
  replayHeapUsedMb: 0,
  replayCpuUserMs: 0,

  /**
   * Render metrics in Prometheus text format
   */
  render() {
    return [
      '# HELP replayd_connections_active Number of active SSE connections',
      '# TYPE replayd_connections_active gauge',
      `replayd_connections_active ${this.connectionsActive}`,
      '',
      '# HELP replayd_events_sent_total Total number of events sent across all streams',
      '# TYPE replayd_events_sent_total counter',
      `replayd_events_sent_total ${this.eventsSentTotal}`,
      '',
      '# HELP replayd_bytes_sent_total Total bytes sent (approximate)',
      '# TYPE replayd_bytes_sent_total counter',
      `replayd_bytes_sent_total ${this.bytesSentTotal}`,
      '',
      '# HELP replayd_stream_errors_total Total number of stream errors',
      '# TYPE replayd_stream_errors_total counter',
      `replayd_stream_errors_total ${this.streamErrorsTotal}`,
      '',
      '# HELP replayd_backpressure_waits_total Total times the stream had to wait for backpressure',
      '# TYPE replayd_backpressure_waits_total counter',
      `replayd_backpressure_waits_total ${this.backpressureWaitsTotal}`,
      '',
      '# HELP replayd_client_too_slow_disconnects_total Total times a connection was dropped due to slow client',
      '# TYPE replayd_client_too_slow_disconnects_total counter',
      `replayd_client_too_slow_disconnects_total ${this.clientTooSlowDisconnectsTotal}`,
      '',
      '# HELP replayd_cache_hits_total Total number of cache hits',
      '# TYPE replayd_cache_hits_total counter',
      `replayd_cache_hits_total ${this.cacheHitsTotal}`,
      '',
      '# HELP replayd_s3_get_ops_total Total number of S3 GET operations',
      '# TYPE replayd_s3_get_ops_total counter',
      `replayd_s3_get_ops_total ${this.s3GetOpsTotal}`,
      '',
      '# HELP replay_stream_requests_total Total number of stream requests',
      '# TYPE replay_stream_requests_total counter',
      `replay_stream_requests_total ${this.streamRequestsTotal}`,
      '',
      '# HELP replay_stream_empty_total Total number of streams that yielded 0 events',
      '# TYPE replay_stream_empty_total counter',
      `replay_stream_empty_total ${this.streamEmptyTotal}`,
      '',
      '# HELP replay_stream_events_total Streamed events total',
      '# TYPE replay_stream_events_total counter',
      `replay_stream_events_total ${this.replayStreamEventsTotal}`,
      '',
      '# HELP replay_stream_last_event_ts Last streamed event timestamp (epoch ms)',
      '# TYPE replay_stream_last_event_ts gauge',
      `replay_stream_last_event_ts ${this.replayStreamLastEventTs}`,
      '',
      '# HELP replay_stream_reconnect_total SSE reconnects (cursor resume requests)',
      '# TYPE replay_stream_reconnect_total counter',
      `replay_stream_reconnect_total ${this.replayStreamReconnectTotal}`,
      '',
      '# HELP replay_queue_depth Replay queue depth (buffered events)',
      '# TYPE replay_queue_depth gauge',
      `replay_queue_depth ${this.replayQueueDepth}`,
      '',
      '# HELP replay_engine_cycles_total Replay engine cycles (rows processed)',
      '# TYPE replay_engine_cycles_total counter',
      `replay_engine_cycles_total ${this.replayEngineCyclesTotal}`,
      '',
      '# HELP replay_backpressure_total Backpressure occurrences (queue depth > 500)',
      '# TYPE replay_backpressure_total counter',
      `replay_backpressure_total ${this.replayBackpressureTotal}`,
      '',
      '# HELP replay_stream_restarts_total Stream restarts (cursor resume requests)',
      '# TYPE replay_stream_restarts_total counter',
      `replay_stream_restarts_total ${this.replayStreamRestartsTotal}`,
      '',
      '# HELP replay_stream_errors_total Stream errors total',
      '# TYPE replay_stream_errors_total counter',
      `replay_stream_errors_total ${this.replayStreamErrorsTotal}`,
      '',
      '# HELP replay_event_loop_lag_ms Event loop lag in ms',
      '# TYPE replay_event_loop_lag_ms gauge',
      `replay_event_loop_lag_ms ${this.replayEventLoopLagMs}`,
      '',
      '# HELP replay_active_streams Active SSE streams',
      '# TYPE replay_active_streams gauge',
      `replay_active_streams ${this.replayActiveStreams}`,
      '',
      '# HELP replay_processing_latency_ms Replay processing latency in ms',
      '# TYPE replay_processing_latency_ms gauge',
      `replay_processing_latency_ms ${this.replayProcessingLatencyMs}`,
      '',
      '# HELP replay_heap_used_mb Heap used in MB',
      '# TYPE replay_heap_used_mb gauge',
      `replay_heap_used_mb ${this.replayHeapUsedMb}`,
      '',
      '# HELP replay_cpu_user_ms CPU user time in ms',
      '# TYPE replay_cpu_user_ms gauge',
      `replay_cpu_user_ms ${this.replayCpuUserMs}`
    ].join('\n');
  }
};

export function startReplayTelemetry() {
  const lagIntervalMs = 100;
  let lastTick = Date.now();
  const lagTimer = setInterval(() => {
    const now = Date.now();
    const drift = now - lastTick - lagIntervalMs;
    replayMetrics.replayEventLoopLagMs = drift > 0 ? drift : 0;
    lastTick = now;
  }, lagIntervalMs);
  lagTimer.unref();

  const usageTimer = setInterval(() => {
    const mem = process.memoryUsage();
    const cpu = process.cpuUsage();
    replayMetrics.replayHeapUsedMb = Number((mem.heapUsed / (1024 * 1024)).toFixed(3));
    replayMetrics.replayCpuUserMs = Number((cpu.user / 1000).toFixed(3));
  }, 1000);
  usageTimer.unref();
}
