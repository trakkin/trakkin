<script lang="ts">
import { Menu, Plus } from "@lucide/svelte";
import { onMount, type Snippet } from "svelte";
import { goto } from "$app/navigation";
import { page } from "$app/state";
import { listWorkspaces, type Workspace } from "$lib/api/workspaces";
import { DropdownMenu, IconButton, Select, Tooltip } from "$lib/components/ui";
import { navigationFor } from "./navigation";
import WorkspaceActivityPopover from "./WorkspaceActivityPopover.svelte";

type Props = {
  children: Snippet;
};

let { children }: Props = $props();

const year = new Date().getFullYear();
let workspaces = $state<Workspace[]>([]);
let workspaceError = $state("");
let workspaceRequestSequence = 0;
let activeWorkspaceId = $derived(
  (page.params as Record<string, string | undefined>).workspaceId ?? page.url.searchParams.get("workspace") ?? "",
);
let appNavigation = $derived(navigationFor(activeWorkspaceId));
let workspaceItems = $derived(workspaces.map((workspace) => ({ value: workspace.id, label: workspace.name })));
let workspacePlaceholder = $derived(
  workspaceError ? "Workspaces unavailable" : workspaceItems.length === 0 ? "No active workspaces" : "Select workspace",
);

onMount(() => {
  async function refreshWorkspaces() {
    const sequence = ++workspaceRequestSequence;
    workspaceError = "";
    try {
      const loadedWorkspaces = (await listWorkspaces()).filter((workspace) => workspace.status === "active");
      if (sequence === workspaceRequestSequence) workspaces = loadedWorkspaces;
    } catch (error) {
      if (sequence === workspaceRequestSequence) {
        workspaceError = error instanceof Error ? error.message : "Workspaces unavailable";
      }
    }
  }
  void refreshWorkspaces();
  window.addEventListener("trakkin:workspaces-changed", refreshWorkspaces);
  return () => window.removeEventListener("trakkin:workspaces-changed", refreshWorkspaces);
});

function selectWorkspace(workspaceId: string) {
  if (workspaceId) void goto(`/w/${workspaceId}/catalog`);
}

function isCurrent(href: string): boolean {
  const pathname = new URL(href, page.url).pathname;
  return page.url.pathname === pathname || (pathname !== "/" && page.url.pathname.startsWith(`${pathname}/`));
}
</script>

<div class="app-canvas h-dvh overflow-hidden lg:p-2">
  <div class="flex h-full min-h-0">
    <aside class="app-sidebar hidden w-61 shrink-0 flex-col px-3 pt-3 pb-2 lg:flex">
      <div class="flex h-8 shrink-0 items-center justify-between gap-3 px-1">
        <div class="flex min-w-0 items-center gap-2">
          <span
            class="flex size-7 items-center justify-center rounded-md border border-border-subtle bg-white/4 shadow-[inset_0_1px_rgb(255_255_255/4%)]"
          >
            <img
              alt=""
              class="size-4 rounded-control"
              src="/trakkin-icon.svg"
            >
          </span>
          <span class="text-heading font-semibold text-text-primary">Trakkin</span>
        </div>
        <WorkspaceActivityPopover workspaceId={activeWorkspaceId} />
      </div>

      <div class="mt-4 flex items-center gap-1.5">
        <Select
          ariaLabel="Active workspace"
          class="min-w-0 flex-1"
          disabled={workspaceItems.length === 0}
          items={workspaceItems}
          onValueChange={selectWorkspace}
          placeholder={workspacePlaceholder}
          triggerClass="min-w-0 w-full border-border-strong bg-white/4 shadow-[inset_0_1px_rgb(255_255_255/3%)]"
          value={activeWorkspaceId}
        />
        <IconButton
          label="Create workspace"
          onclick={() => goto("/workspaces/new")}
          size="sm"
          title="Create workspace"
        >
          <Plus
            aria-hidden="true"
            size={14}
          />
        </IconButton>
      </div>

      <nav
        aria-label="Primary navigation"
        class="mt-5 flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto"
      >
        {#each appNavigation as group (group.label)}
          <div>
            {#if group.label}
              <p class="mb-1 flex h-5 items-center px-2 text-meta font-[560] text-text-disabled">
                {group.label}
              </p>
            {/if}
            <div class="space-y-1">
              {#each group.items as item (item.label)}
                {@const Icon = item.icon}
                {#if item.href}
                  {@const current = isCurrent(item.href)}
                  <a
                    aria-current={current ? "page" : undefined}
                    class="relative flex h-8 items-center gap-2.5 rounded-md px-2 text-label font-[520] transition-[background-color,color] duration-(--motion-fast) {current ? 'bg-white/6 text-text-primary shadow-[inset_0_0_0_1px_rgb(255_255_255/4%)]' : 'text-text-muted hover:bg-white/4 hover:text-text-secondary'}"
                    href={item.href}
                  >
                    {#if current}
                      <span
                        aria-hidden="true"
                        class="absolute top-2 bottom-2 -left-0.5 w-0.5 rounded-full bg-accent"
                      ></span>
                    {/if}
                    <Icon
                      aria-hidden="true"
                      class={current ? "text-accent" : "text-text-muted"}
                      size={14}
                      strokeWidth={2}
                    />
                    <span>{item.label}</span>
                  </a>
                {:else}
                  <Tooltip.Root>
                    <Tooltip.Trigger>
                      {#snippet child({ props })}
                        <button
                          {...props}
                          aria-disabled="true"
                          class="flex h-8 w-full cursor-default items-center gap-2.5 rounded-md px-2 text-label font-[520] text-text-muted opacity-45 outline-none focus-visible:ring-1 focus-visible:ring-accent"
                          type="button"
                        >
                          <Icon
                            aria-hidden="true"
                            size={14}
                            strokeWidth={2}
                          />
                          <span>{item.label}</span>
                        </button>
                      {/snippet}
                    </Tooltip.Trigger>
                    <Tooltip.Content side="right">Coming soon</Tooltip.Content>
                  </Tooltip.Root>
                {/if}
              {/each}
            </div>
          </div>
        {/each}
      </nav>

      <footer
        class="mt-3 flex h-8 shrink-0 items-center justify-end border-t border-border-subtle px-2 pt-2 text-meta text-text-disabled"
      >
        <a
          class="transition-colors duration-(--motion-fast) hover:text-text-secondary"
          href="https://trakkin.dev"
          title="Made by @eliasbenb"
          >&copy; {year}</a
        >
      </footer>
    </aside>

    <section
      class="app-workspace flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden lg:rounded-panel lg:border lg:border-border-subtle"
    >
      <header class="flex h-11 shrink-0 items-center gap-2.5 border-b border-border-subtle px-3 lg:hidden">
        <img
          alt=""
          class="size-4 rounded-control"
          src="/trakkin-icon.svg"
        >
        <Select
          ariaLabel="Active workspace"
          class="ml-1 min-w-0 flex-1"
          disabled={workspaceItems.length === 0}
          items={workspaceItems}
          onValueChange={selectWorkspace}
          placeholder={workspacePlaceholder}
          triggerClass="min-w-0 w-full max-w-52 border-0 bg-transparent px-1.5 hover:bg-white/4"
          value={activeWorkspaceId}
        />
        <WorkspaceActivityPopover workspaceId={activeWorkspaceId} />
        <DropdownMenu.Root>
          <DropdownMenu.Trigger>
            {#snippet child({ props })}
              <IconButton
                {...props}
                label="Open navigation"
                size="sm"
              >
                <Menu
                  aria-hidden="true"
                  size={15}
                />
              </IconButton>
            {/snippet}
          </DropdownMenu.Trigger>
          <DropdownMenu.Content
            align="end"
            class="min-w-48"
          >
            {#each appNavigation as group (group.label)}
              {#each group.items as item (item.label)}
                {#if item.href}
                  {@const Icon = item.icon}
                  {@const current = isCurrent(item.href)}
                  <DropdownMenu.Item
                    aria-current={current ? "page" : undefined}
                    class="flex h-8 cursor-default items-center gap-2 rounded-control px-2 outline-none data-highlighted:bg-white/5 {current ? 'bg-white/6 text-text-primary' : ''}"
                    onclick={() => item.href && goto(item.href)}
                  >
                    <Icon
                      aria-hidden="true"
                      class={current ? "text-accent" : undefined}
                      size={14}
                    />
                    <span>{item.label}</span>
                  </DropdownMenu.Item>
                {/if}
              {/each}
            {/each}
            <hr class="my-1 border-border-subtle">
            <DropdownMenu.Item
              class="flex h-8 cursor-default items-center gap-2 rounded-control px-2 outline-none data-highlighted:bg-white/5"
              onclick={() => goto("/workspaces/new")}
            >
              <Plus
                aria-hidden="true"
                size={14}
              />
              <span>Create workspace</span>
            </DropdownMenu.Item>
          </DropdownMenu.Content>
        </DropdownMenu.Root>
      </header>
      <main class="min-h-0 flex-1 overflow-hidden">{@render children()}</main>
    </section>
  </div>
</div>
