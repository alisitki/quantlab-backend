'use client';

import Link from 'next/link';
import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/api';

export default function ExperimentsPage() {
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    apiFetch('/v1/experiments')
      .then(setData)
      .catch((e: any) => setError(e.message || 'Failed'));
  }, []);

  if (error) return <div className="card">{error}</div>;
  if (!data) return <div className="card">Loading...</div>;

  return (
    <div className="card">
      <div className="section-title">Experiments</div>
      {data.experiments.length === 0 && <div className="small">No experiments</div>}
      <table className="table">
        <thead>
          <tr>
            <th>exp_id</th>
            <th>strategy</th>
            <th>dataset</th>
            <th>validated_jobs</th>
          </tr>
        </thead>
        <tbody>
          {data.experiments.map((e: any) => (
            <tr key={`${e.exp_id}-${e.created_at}`}>
              <td><Link href={`/quantlab/experiments/${e.exp_id}`}>{e.exp_id}</Link></td>
              <td>{e.strategy_id}</td>
              <td>{e.dataset.symbol}@{e.dataset.date}</td>
              <td>{e.validated_jobs}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
