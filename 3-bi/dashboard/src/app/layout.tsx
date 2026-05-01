import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "D-DEE Dashboard",
  description: "Precision Scaling dashboard product for D-DEE.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
