/**
 * Strategyd Control Route
 * POST /control
 */

export default async function controlRoutes(fastify, options) {
  const { runner } = options;
  
  fastify.post('/control', async (request, reply) => {
    const { action } = request.body || {};
    
    if (!action) {
      return reply.code(400).send({ error: 'MISSING_ACTION' });
    }
    
    switch (action) {
      case 'pause':
        await runner.pause();
        return { success: true, status: 'PAUSED' };
        
      case 'resume':
        await runner.resume();
        return { success: true, status: 'RUNNING' };
        
      case 'kill':
        const fill = await runner.kill();
        return { 
          ok: true, 
          action: 'kill', 
          closed: true,
          message: fill ? 'Positions closed' : 'No open positions',
          fill 
        };

        
      default:
        return reply.code(400).send({ 
          error: 'INVALID_ACTION', 
          message: 'Valid actions: pause, resume, kill' 
        });
    }
  });
}
