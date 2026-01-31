import Link from 'next/link';

export default function QuantLabLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="container">
      <div className="header">
        <div className="brand">QuantLab Console</div>
        <nav className="nav">
          <Link href="/quantlab/jobs">Jobs</Link>
          <Link href="/quantlab/experiments">Experiments</Link>
          <Link href="/quantlab/candidates">Candidates</Link>
          <Link href="/quantlab/settings">Settings</Link>
        </nav>
      </div>
      {children}
    </div>
  );
}
