/**
 * Feature extraction routes
 * POST /features
 * GET /features/:job_id
 */

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const ID_RE = /^[a-f0-9]{64}$/;

export default async function featureRoutes(fastify, options) {
  const { orchestrator, jobStore } = options;

  fastify.post('/features', async (request, reply) => {
    const body = request.body || {};
    const requestId = request.id || 'req_unknown';

    const error = validateJob(body);
    if (error) {
      return reply.code(400).send({ error: 'INVALID_JOB', message: error });
    }

    const job = await orchestrator.submit(body);
    console.log(`[FeatureRoutes] component=featurexd action=job_submit job_id=${job.job_id} request_id=${requestId}`);

    return { job_id: job.job_id, state: job.state };
  });

  fastify.get('/features/:job_id', async (request, reply) => {
    const jobId = request.params.job_id;
    if (!ID_RE.test(jobId)) {
      return reply.code(400).send({ error: 'INVALID_JOB_ID', id: jobId });
    }

    const job = await jobStore.get(jobId);
    if (!job) {
      return reply.code(404).send({ error: 'JOB_NOT_FOUND', id: jobId });
    }

    return job;
  });
}

function validateJob(body) {
  if (!Array.isArray(body.date_range) || body.date_range.length !== 2) return 'date_range must be [start,end]';
  if (!DATE_RE.test(body.date_range[0]) || !DATE_RE.test(body.date_range[1])) return 'date_range must be YYYY-MM-DD';
  if (!Array.isArray(body.streams) || body.streams.length === 0) return 'streams required';
  if (!Array.isArray(body.symbols) || body.symbols.length === 0) return 'symbols required';
  if (!body.feature_set_id || typeof body.feature_set_id !== 'string') return 'feature_set_id required';
  if (!body.feature_set_version || typeof body.feature_set_version !== 'string') return 'feature_set_version required';
  if (!Number.isFinite(body.label_horizon_sec)) return 'label_horizon_sec must be number';
  return null;
}
