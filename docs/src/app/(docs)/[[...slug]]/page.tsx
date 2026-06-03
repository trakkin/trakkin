import {
  DocsBody,
  DocsDescription,
  DocsPage,
  DocsTitle,
  MarkdownCopyButton,
  ViewOptionsPopover,
} from "fumadocs-ui/layouts/docs/page";
import { createRelativeLink } from "fumadocs-ui/mdx";
import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { getMDXComponents } from "@/components/mdx.tsx";
import { gitConfig } from "@/lib/shared.ts";
import { getPageImageUrl, getPageMarkdownUrl, source } from "@/lib/source.ts";

export default async function Page(props: PageProps<"/[[...slug]]">) {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page && !params.slug) {
    return (
      <DocsPage toc={[]}>
        <DocsTitle>Trakkin documentation</DocsTitle>
        <DocsDescription>Track and sync everything you watch, read, play, and listen to.</DocsDescription>
      </DocsPage>
    );
  }
  if (!page) notFound();

  const MDX = page.data.body;
  const markdownUrl = getPageMarkdownUrl(page).url;

  return (
    <DocsPage full={page.data.full} toc={page.data.toc}>
      <DocsTitle>{page.data.title}</DocsTitle>
      <DocsDescription className="mb-0">{page.data.description}</DocsDescription>
      <div className="flex flex-row gap-2 items-center border-b pb-6">
        <MarkdownCopyButton markdownUrl={markdownUrl} />
        <ViewOptionsPopover
          githubUrl={`https://github.com/${gitConfig.user}/${gitConfig.repo}/blob/${gitConfig.ref}/docs/content/${page.path}`}
          markdownUrl={markdownUrl}
        />
      </div>
      <DocsBody>
        <MDX
          components={getMDXComponents({
            a: createRelativeLink(source, page),
          })}
        />
      </DocsBody>
    </DocsPage>
  );
}

export const dynamicParams = false;

export function generateStaticParams() {
  return [{ slug: undefined }, ...source.generateParams().filter((params) => params.slug.length > 0)];
}

export async function generateMetadata(props: PageProps<"/[[...slug]]">): Promise<Metadata> {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page && !params.slug) {
    return {
      title: "Documentation",
      description: "Track and sync everything you watch, read, play, and listen to.",
    };
  }
  if (!page) notFound();

  return {
    title: page.data.title,
    description: page.data.description,
    openGraph: {
      images: getPageImageUrl(page).url,
    },
  };
}
