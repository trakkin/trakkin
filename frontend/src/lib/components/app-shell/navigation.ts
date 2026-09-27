import { Blocks, Cable, Library, PackageOpen, ScrollText } from "@lucide/svelte";
import type { Component } from "svelte";
import { dev } from "$app/environment";

export type AppNavigationItem = {
  href?: string;
  icon: Component;
  label: string;
};

export type AppNavigationGroup = {
  items: AppNavigationItem[];
  label: string;
};

export function navigationFor(workspaceId?: string): AppNavigationGroup[] {
  const workspaceItems: AppNavigationItem[] = workspaceId
    ? [{ href: `/w/${workspaceId}/catalog`, icon: Library, label: "Catalog" }]
    : [];
  return [
    ...(workspaceItems.length > 0 ? [{ label: "Workspace", items: workspaceItems }] : []),
    {
      label: "",
      items: [
        {
          href: workspaceId ? `/connections?workspace=${workspaceId}` : "/connections",
          icon: Cable,
          label: "Connections",
        },
        { href: "/providers", icon: PackageOpen, label: "Providers" },
        { href: "/logs", icon: ScrollText, label: "Logs" },
      ],
    },
    ...(dev
      ? [
          {
            label: "Development",
            items: [{ href: "/ui", icon: Blocks, label: "UI" }],
          },
        ]
      : []),
  ];
}
