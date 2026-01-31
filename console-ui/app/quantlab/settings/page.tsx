'use client';

import { useEffect, useState } from 'react';

export default function SettingsPage() {
  const [token, setToken] = useState('');
  const [url, setUrl] = useState('');
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    setToken(localStorage.getItem('observer_token') || '');
    setUrl(localStorage.getItem('observer_url') || process.env.NEXT_PUBLIC_OBSERVER_URL || 'http://127.0.0.1:3011');
  }, []);

  const save = () => {
    localStorage.setItem('observer_token', token);
    localStorage.setItem('observer_url', url);
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  return (
    <div className="card">
      <div className="section-title">Observer Settings</div>
      <label className="small">Observer Base URL</label>
      <input className="input" value={url} onChange={(e) => setUrl(e.target.value)} />
      <div style={{ height: 12 }} />
      <label className="small">Bearer Token</label>
      <input className="input" value={token} onChange={(e) => setToken(e.target.value)} />
      <div style={{ height: 12 }} />
      <button onClick={save}>Save</button>
      {saved && <div className="toast">Settings saved</div>}
    </div>
  );
}
