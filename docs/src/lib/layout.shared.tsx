import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import Image from "next/image";
import { appName, gitConfig, withDocsBasePath } from "./shared.ts";
import { currentVersionLabel, docsVersions } from "./versions.ts";

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <span className="flex items-center gap-2">
          <span className="flex size-7 items-center justify-center rounded-md border border-fd-border bg-white/4">
            <Image alt="" className="h-4 w-auto" height={16} src={withDocsBasePath("/trakkin-icon.svg")} width={12} />
          </span>
          <span>{appName}</span>
        </span>
      ),
    },
    githubUrl: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
    links: [
      {
        type: "menu",
        text: currentVersionLabel,
        items: docsVersions.map((version) => ({
          type: "main",
          text: version.label,
          url: version.url,
        })),
      },
    ],
    themeSwitch: {
      enabled: false,
    },
  };
}
