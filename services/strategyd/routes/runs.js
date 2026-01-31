/**
 * Strategyd Runs Route
 * GET /runs, GET /run/:id
 */

export default async function runsRoutes(fastify, options) {
  const { runner } = options;
  const manifestManager = runner.getManifestManager();
  
  // GET /runs - List recent runs
  fastify.get('/runs', async () => {
    const list = await manifestManager.list();
    return { count: list.length, runs: list };
  });

  // GET /run/:id - Get specific manifest
  fastify.get('/run/:id', async (request, reply) => {
    const { id } = request.params;
    const manifest = await manifestManager.get(id);
    
    if (!manifest) {
      return reply.code(404).send({ error: 'RUN_NOT_FOUND', id });
    }
    
    return manifest;
  });
}
