import "./globals.css";
import type { Metadata, Viewport } from "next";

export const metadata: Metadata = {
  title: "Memo",
  description: "Personal augmented memory (local-first)",
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark" suppressHydrationWarning>
      <body className="relative min-h-dvh min-w-0 max-w-full overflow-hidden">
        <div className="relative z-0 min-h-dvh min-h-[100dvh] min-w-0">{children}</div>
        <div className="memo-nebula-veil" aria-hidden />
      </body>
    </html>
  );
}
