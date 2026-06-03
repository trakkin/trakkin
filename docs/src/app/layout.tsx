import { RootProvider } from "fumadocs-ui/provider/next";
import type { Metadata } from "next";
import { Inter } from "next/font/google";
import { docsUrl, withDocsBasePath } from "@/lib/shared.ts";
import "./global.css";

const inter = Inter({
  subsets: ["latin"],
});

export const metadata: Metadata = {
  metadataBase: docsUrl("/"),
  alternates: {
    canonical: docsUrl("/"),
  },
  title: {
    default: "Trakkin",
    template: "Trakkin &middot; %s",
  },
  description: "Track and sync everything you watch, read, play, and listen to.",
};

export default function Layout({ children }: LayoutProps<"/">) {
  return (
    <html className={`${inter.className} dark`} lang="en" suppressHydrationWarning>
      <body className="flex flex-col min-h-screen">
        <RootProvider
          search={{ options: { api: withDocsBasePath("/api/search"), type: "static" } }}
          theme={{ enabled: false }}
        >
          {children}
        </RootProvider>
      </body>
    </html>
  );
}
