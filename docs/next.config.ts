import { createMDX } from "fumadocs-mdx/next";
import type { NextConfig } from "next";

const withMDX = createMDX();
const configuredBasePath = process.env.TRAKKIN_DOCS_BASE_PATH?.trim() ?? "";
const basePath =
  configuredBasePath === "" || configuredBasePath === "/" ? "" : `/${configuredBasePath.replace(/^\/+|\/+$/g, "")}`;

const config: NextConfig = {
  basePath,
  images: {
    unoptimized: true,
  },
  output: "export",
  reactStrictMode: true,
  trailingSlash: true,
};

export default withMDX(config);
