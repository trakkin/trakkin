import { docsBasePath, siteUrl } from "./shared.ts";

interface DocsVersion {
  label: string;
  path: string;
}

export const currentVersionLabel = process.env.TRAKKIN_DOCS_VERSION_LABEL ?? "Development";

function configuredVersions(): DocsVersion[] {
  const serialized = process.env.TRAKKIN_DOCS_VERSIONS;
  if (!serialized) {
    return [
      {
        label: currentVersionLabel,
        path: docsBasePath === "" ? "/" : `${docsBasePath}/`,
      },
    ];
  }

  const versions: unknown = JSON.parse(serialized);
  if (!Array.isArray(versions)) {
    throw new Error("TRAKKIN_DOCS_VERSIONS must be a JSON array");
  }

  return versions.map((version) => {
    if (
      typeof version !== "object" ||
      version === null ||
      !("label" in version) ||
      !("path" in version) ||
      typeof version.label !== "string" ||
      typeof version.path !== "string" ||
      !version.path.startsWith("/")
    ) {
      throw new Error("each documentation version requires string label and absolute path fields");
    }
    return { label: version.label, path: version.path };
  });
}

export const docsVersions = configuredVersions().map((version) => ({
  ...version,
  url: siteUrl(version.path).href,
}));
