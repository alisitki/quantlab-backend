/**
 * Label routes
 * POST /labels
 * GET /labels/:job_id
 */

const ID_RE = /^[a-f0-9]{64}$/;

export default async function labelRoutes(fastify, options) {
  const { orchestrator, jobStore } = options;

  fastify.post('/labels', async (request, reply) => {
    const body = request.body || {};
    const requestId = request.id || 'req_unknown';

    const error = validateJob(body);
    if (error) {
      return reply.code(400).send({ error: 'INVALID_JOB', message: error });
    }

    const job = await orchestrator.submit(body);
    console.log(`[LabelRoutes] component=labeld action=job_submit job_id=${job.job_id} request_id=${requestId}`);

    return { job_id: job.job_id, state: job.state };
  });

  fastify.get('/labels/:job_id', async (request, reply) => {
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
  if (!body.feature_job_id || typeof body.feature_job_id !== 'string') return 'feature_job_id required';
  if (!body.label_set_id || typeof body.label_set_id !== 'string') return 'label_set_id required';
  if (!body.label_set_version || typeof body.label_set_version !== 'string') return 'label_set_version required';
  if (!Number.isFinite(body.label_horizon_sec)) return 'label_horizon_sec must be number';
  return null;
}
