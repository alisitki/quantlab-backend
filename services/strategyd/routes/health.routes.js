import { ActiveHealth } from '../runtime/ActiveHealth.js';

export default async function healthRoutes(fastify, options) {
  const { runner } = options;

  fastify.get('/health/active', async (request, reply) => {
    try {
      const strategyId = runner?.getStrategyId ? runner.getStrategyId() : null;
      const seed = runner?.getStrategySeed ? runner.getStrategySeed() : null;
      const health = new ActiveHealth({
        strategyId: strategyId || null,
        seed: seed || null
      });
      const snapshot = health.getSnapshot();
      return reply.code(200).send(snapshot);
    } catch {
      return reply.code(200).send({
        active_enabled: false,
        strategy_id: null,
        seed: null,
        active_config_present: false,
        limits: { max_weight: null, daily_cap: null },
        guards: { kill_switch_required: true, safety_audit_required: true },
        provenance: {
          active_config_hash: null,
          decision_hash: null,
          triad_report_hash: null
        }
      });
    }
  });
}
