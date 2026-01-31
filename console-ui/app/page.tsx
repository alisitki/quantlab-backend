import Link from 'next/link';

export default function HomePage() {
  return (
    <div className="container">
      <div className="card">
        <div className="section-title">QuantLab Console</div>
        <p className="small">Observer API consumer UI.</p>
        <Link href="/quantlab/jobs">Go to Jobs</Link>
      </div>
    </div>
  );
}
