import { loader } from "fumadocs-core/source";
import { lucideIconsPlugin } from "fumadocs-core/source/lucide-icons";
import { metaSchema, pageSchema } from "fumadocs-core/source/schema";
import { defineDocs } from "fumadocs-mdx/macro";
import { docsContentRoute, docsImageRoute, docsRoute, docsUrl, withDocsBasePath } from "./shared.ts";

const docs = defineDocs({
  dir: "content",
  docs: {
    schema: pageSchema,
    postprocess: {
      includeProcessedMarkdown: true,
    },
  },
  meta: {
    schema: metaSchema,
  },
});

export const source = loader({
  baseUrl: docsRoute,
  source: docs.toFumadocsSource(),
  plugins: [lucideIconsPlugin()],
});

export function getPageImageUrl(page: (typeof source)["$inferPage"]) {
  const segments = [...page.slugs, "image.png"];

  return {
    segments,
    url: withDocsBasePath(`/${[page.locale, ...docsImageRoute.split("/"), ...segments].filter(Boolean).join("/")}`),
  };
}

export function getPageMarkdownUrl(page: (typeof source)["$inferPage"]) {
  const segments = [...page.slugs, "content.md"];

  return {
    segments,
    url: withDocsBasePath(`/${[page.locale, ...docsContentRoute.split("/"), ...segments].filter(Boolean).join("/")}`),
  };
}

export async function getLLMText(page: (typeof source)["$inferPage"]) {
  const processed = await page.data.getText("processed");

  return `# ${page.data.title} (${docsUrl(page.url).href})

${processed}`;
}
