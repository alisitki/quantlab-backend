/**
 * Strategyd Trades Route
 * GET /trades
 */

export default async function tradesRoutes(fastify, options) {
  const { runner } = options;
  
  fastify.get('/trades', async () => {
    const snapshot = runner.getSnapshot();
    return {
      count: snapshot.fills.length,
      trades: snapshot.fills
    };
  });
}
