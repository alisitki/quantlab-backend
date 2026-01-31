'use client';

import Link from 'next/link';
import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/api';

export default function CandidatesPage() {
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    apiFetch('/v1/candidates')
      .then(setData)
      .catch((e: any) => setError(e.message || 'Failed'));
  }, []);

  if (error) return <div className="card">{error}</div>;
  if (!data) return <div className="card">Loading...</div>;

  return (
    <div className="card">
      <div className="section-title">Candidates</div>
      <table className="table">
        <thead>
          <tr>
            <th>candidate_id</th>
            <th>strategy</th>
            <th>tick_pnl</th>
            <th>max_dd</th>
            <th>trades</th>
          </tr>
        </thead>
        <tbody>
          {data.candidates.map((c: any) => (
            <tr key={c.candidate_id}>
              <td><Link href={`/quantlab/candidates/${c.candidate_id}`}>{c.candidate_id}</Link></td>
              <td>{c.strategy_id}</td>
              <td>{c.scores?.tick_pnl_pct}</td>
              <td>{c.scores?.max_dd}</td>
              <td>{c.scores?.trades}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
