export const appName = "Trakkin";
export const docsRoute = "/";
export const docsImageRoute = "/og/docs";
export const docsContentRoute = "/llms.mdx/docs";
export const docsOrigin = (process.env.TRAKKIN_DOCS_ORIGIN ?? "https://trakkin.dev").replace(/\/$/, "");

const configuredBasePath = process.env.TRAKKIN_DOCS_BASE_PATH?.trim() ?? "";
export const docsBasePath =
  configuredBasePath === "" || configuredBasePath === "/" ? "" : `/${configuredBasePath.replace(/^\/+|\/+$/g, "")}`;

export function withDocsBasePath(path: string) {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  if (docsBasePath === "") return normalizedPath;
  if (normalizedPath === "/") return `${docsBasePath}/`;
  return `${docsBasePath}${normalizedPath}`;
}

export function docsUrl(path: string) {
  return new URL(withDocsBasePath(path), `${docsOrigin}/`);
}

export function siteUrl(path: string) {
  return new URL(path, `${docsOrigin}/`);
}

export const gitConfig = {
  user: "trakkin",
  repo: "trakkin",
  ref: process.env.TRAKKIN_DOCS_GIT_REF ?? "main",
};
