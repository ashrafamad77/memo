import "./globals.css";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Memo",
  description: "Personal augmented memory (local-first)",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-screen">{children}</body>
    </html>
  );
}
