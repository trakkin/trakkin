<script lang="ts">
import type { Snippet } from "svelte";
import { page } from "$app/state";
import { PageHeader, Tabs } from "$lib";

type Props = {
  actions?: Snippet;
};

let { actions }: Props = $props();

const tabs = [
  { href: "/mappings", label: "Mappings" },
  { href: "/mappings/sources", label: "Sources" },
];

let currentHref = $derived(currentItem(page.url.pathname)?.href);

function currentItem(pathname: string): (typeof tabs)[number] | undefined {
  return tabs
    .filter((item) => pathname === item.href || pathname.startsWith(`${item.href}/`))
    .reduce<(typeof tabs)[number] | undefined>(
      (current, item) => (!current || item.href.length > current.href.length ? item : current),
      undefined,
    );
}
</script>

<PageHeader
  {actions}
  title="Mappings"
>
  {#snippet below()}
    <nav
      aria-label="Mapping views"
      class={["-mb-3.5", Tabs.listClasses()]}
    >
      {#each tabs as tab (tab.href)}
        {@const current = currentHref === tab.href}
        <a
          aria-current={current ? "page" : undefined}
          class={Tabs.triggerClasses()}
          data-state={current ? "active" : "inactive"}
          href={tab.href}
        >
          {tab.label}
        </a>
      {/each}
    </nav>
  {/snippet}
</PageHeader>
