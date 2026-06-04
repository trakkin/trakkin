import { createFromSource } from "fumadocs-core/search/server";
import { source } from "@/lib/source.ts";

const search = createFromSource(source);
export const dynamic = "force-static";

export async function GET() {
  return Response.json(await search.export());
}
