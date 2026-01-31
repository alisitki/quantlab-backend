import './globals.css';

export const metadata = {
  title: 'QuantLab Console',
  description: 'Observer API console',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
