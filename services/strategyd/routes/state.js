/**
 * Strategyd State Route
 * GET /state
 */

export default async function stateRoutes(fastify, options) {
  const { runner } = options;
  
  fastify.get('/state', async () => {
    const snapshot = runner.getSnapshot();
    const stats = runner.getStats();
    
    return {
      runId: stats.runId,
      status: stats.status,
      symbol: snapshot.symbol,

      equity: snapshot.equity,
      totalRealizedPnl: snapshot.totalRealizedPnl,
      unrealizedPnl: snapshot.unrealizedPnl,
      positions: snapshot.positions,
      eventCount: stats.eventCount,
      signalCount: stats.signalCount,
      lastTs: stats.lastTs
    };
  });
}
