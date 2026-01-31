'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../../lib/api';

export default function CandidateDetail({ params }: { params: { candidate_id: string } }) {
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    apiFetch(`/v1/candidates/${params.candidate_id}`)
      .then(setData)
      .catch((e: any) => setError(e.message || 'Failed'));
  }, [params.candidate_id]);

  if (error) return <div className="card">{error}</div>;
  if (!data) return <div className="card">Loading...</div>;

  const candidate = data.candidate;
  const packPath = data.pack_path;

  return (
    <div className="card">
      <div className="section-title">Candidate: {candidate.candidate_id}</div>
      {packPath && (
        <div className="small">Pack path: {packPath}</div>
      )}
      {packPath && (
        <div className="small">Repro: cd {packPath} && ./repro.sh</div>
      )}
      <div style={{ height: 10 }} />
      <div className="code">{JSON.stringify(candidate, null, 2)}</div>
    </div>
  );
}
