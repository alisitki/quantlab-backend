'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/api';

const statusOptions = ['all', 'queued', 'running', 'done', 'failed'] as const;

export default function JobsPage() {
  const [status, setStatus] = useState<typeof statusOptions[number]>('all');
  const [jobs, setJobs] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [selectedJob, setSelectedJob] = useState<any | null>(null);
  const [toast, setToast] = useState('');

  const [sweepSpec, setSweepSpec] = useState('');
  const [sweepExpId, setSweepExpId] = useState('');
  const [sweepConcurrency, setSweepConcurrency] = useState('1');
  const [promoteExpId, setPromoteExpId] = useState('');
  const [packCandidateId, setPackCandidateId] = useState('');

  const load = async () => {
    setLoading(true);
    setError('');
    try {
      const query = status === 'all' ? '' : `?status=${status}`;
      const data = await apiFetch(`/v1/jobs${query}`);
      setJobs(data.jobs || []);
    } catch (e: any) {
      setError(e.message || 'Failed to load');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, [status]);

  const openTimeline = async (jobId: string) => {
    try {
      const data = await apiFetch(`/v1/jobs/${jobId}`);
      setSelectedJob(data);
    } catch (e: any) {
      setToast(e.message || 'Timeline failed');
    }
  };

  const enqueue = async (path: string, payload: any) => {
    try {
      const data = await apiFetch(path, { method: 'POST', body: JSON.stringify(payload) });
      setToast(`Enqueued ${data.job_id}`);
      load();
    } catch (e: any) {
      setToast(e.message || 'Enqueue failed');
    }
  };

  const shortPayload = (job: any) => {
    const payload = job.payload || {};
    if (payload.exp_id) return `exp_id=${payload.exp_id}`;
    if (payload.candidate_id) return `candidate_id=${payload.candidate_id}`;
    if (payload.spec?.strategy_id) return `strategy=${payload.spec.strategy_id}`;
    return JSON.stringify(payload).slice(0, 60);
  };

  return (
    <div className="grid">
      <div className="card" style={{ gridColumn: '1 / -1' }}>
        <div className="section-title">Jobs</div>
        <div style={{ display: 'flex', gap: 8, marginBottom: 10, flexWrap: 'wrap' }}>
          {statusOptions.map(s => (
            <button key={s} onClick={() => setStatus(s)}>{s}</button>
          ))}
        </div>
        {loading && <div className="small">Loading...</div>}
        {error && <div className="small">{error}</div>}
        {!loading && jobs.length === 0 && <div className="small">No jobs</div>}
        <table className="table">
          <thead>
            <tr>
              <th>created_at</th>
              <th>type</th>
              <th>status</th>
              <th>attempt</th>
              <th>payload</th>
              <th>artifacts</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map(j => (
              <tr key={j.job_id} onClick={() => openTimeline(j.job_id)} style={{ cursor: 'pointer' }}>
                <td>{j.created_at}</td>
                <td>{j.type}</td>
                <td>{j.status}</td>
                <td>{j.attempt}</td>
                <td className="small">{shortPayload(j)}</td>
                <td className="small">{j.artifacts?.leaderboard_path || j.artifacts?.candidate_dir || '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="card">
        <div className="section-title">Enqueue Sweep</div>
        <textarea className="input" rows={6} placeholder="spec JSON" value={sweepSpec} onChange={(e) => setSweepSpec(e.target.value)} />
        <div style={{ height: 8 }} />
        <input className="input" placeholder="exp_id (optional)" value={sweepExpId} onChange={(e) => setSweepExpId(e.target.value)} />
        <div style={{ height: 8 }} />
        <input className="input" placeholder="concurrency" value={sweepConcurrency} onChange={(e) => setSweepConcurrency(e.target.value)} />
        <div style={{ height: 8 }} />
        <button onClick={() => enqueue('/v1/jobs/enqueue/sweep', sweepSpec ? JSON.parse(sweepSpec) : {})}>Enqueue</button>
      </div>

      <div className="card">
        <div className="section-title">Enqueue Promote</div>
        <input className="input" placeholder="exp_id" value={promoteExpId} onChange={(e) => setPromoteExpId(e.target.value)} />
        <div style={{ height: 8 }} />
        <button onClick={() => enqueue('/v1/jobs/enqueue/promote', { exp_id: promoteExpId })}>Enqueue</button>
      </div>

      <div className="card">
        <div className="section-title">Enqueue Pack</div>
        <input className="input" placeholder="candidate_id" value={packCandidateId} onChange={(e) => setPackCandidateId(e.target.value)} />
        <div style={{ height: 8 }} />
        <button onClick={() => enqueue('/v1/jobs/enqueue/pack', { candidate_id: packCandidateId })}>Enqueue</button>
      </div>

      {selectedJob && (
        <div className="modal" onClick={() => setSelectedJob(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="section-title">Job Timeline</div>
            <div className="code">{JSON.stringify(selectedJob, null, 2)}</div>
          </div>
        </div>
      )}

      {toast && <div className="toast">{toast}</div>}
    </div>
  );
}
