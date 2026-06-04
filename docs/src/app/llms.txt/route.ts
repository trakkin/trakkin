import { docsUrl } from "@/lib/shared.ts";
import { source } from "@/lib/source.ts";

export const dynamic = "force-static";

export function GET() {
  const entries = source.getPages().map((page) => {
    const description = page.data.description ? `: ${page.data.description}` : "";
    return `- [${page.data.title}](${docsUrl(page.url).href})${description}`;
  });
  return new Response(["# Trakkin documentation", "", ...entries].join("\n"));
}
