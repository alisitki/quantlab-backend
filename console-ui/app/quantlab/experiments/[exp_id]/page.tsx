'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../../lib/api';

export default function ExperimentDetail({ params }: { params: { exp_id: string } }) {
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    apiFetch(`/v1/experiments/${params.exp_id}`)
      .then(setData)
      .catch((e: any) => setError(e.message || 'Failed'));
  }, [params.exp_id]);

  if (error) return <div className="card">{error}</div>;
  if (!data) return <div className="card">Loading...</div>;

  const rows = data.rows || [];

  return (
    <div className="card">
      <div className="section-title">Experiment: {data.exp_id}</div>
      <div className="small">leaderboard_hash: {data.leaderboard_hash}</div>
      <table className="table">
        <thead>
          <tr>
            <th>rank</th>
            <th>params</th>
            <th>tick_pnl</th>
            <th>snapshot_pnl</th>
            <th>max_dd</th>
            <th>trades</th>
            <th>validation</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r: any) => (
            <tr key={r.params_hash}>
              <td>{r.rank}</td>
              <td className="small">{r.params_short}</td>
              <td>{r.tick?.pnl_pct ?? '-'}</td>
              <td>{r.snapshot?.pnl_pct ?? '-'}</td>
              <td>{r.snapshot?.max_dd ?? '-'}</td>
              <td>{r.snapshot?.trades ?? '-'}</td>
              <td>{r.validation_status}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
