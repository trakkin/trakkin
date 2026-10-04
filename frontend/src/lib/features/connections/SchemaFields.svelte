<script lang="ts">
import { KeyRound } from "@lucide/svelte";
import { Select, TextInput } from "$lib";
import type { AdapterField } from "$lib/api/connections";
import { type FieldDrafts, fieldItems, fieldValue } from "./fields";

let {
  id,
  fields,
  drafts,
  missing = [],
  savedSecrets,
  onupdate,
}: {
  id: string;
  fields: AdapterField[];
  drafts: FieldDrafts;
  missing?: string[];
  savedSecrets?: string[];
  onupdate: (key: string, value: string) => void;
} = $props();
</script>

{#if fields.length > 0}
  <div class="grid gap-3 sm:grid-cols-2">
    {#each fields as field (field.key)}
      {@const saved = field.secret && savedSecrets?.includes(field.key)}
      {@const items = fieldItems(field)}
      <label
        class="grid min-w-0 gap-1.5 text-label text-text-secondary"
        for={`${id}-${field.key}`}
      >
        <span class="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
          <span
            >{field.displayName}
            {#if field.required}
              <span class="text-danger"> *</span>
            {/if}</span
          >
          {#if saved}
            <span class="inline-flex items-center gap-1 text-meta font-[520] text-success">
              <KeyRound
                aria-hidden="true"
                size={11}
              />Secret saved
            </span>
          {/if}
        </span>
        {#if items.length > 0}
          <Select
            ariaInvalid={missing.includes(field.key)}
            ariaLabel={field.displayName}
            {items}
            onValueChange={(value) => onupdate(field.key, value)}
            placeholder={field.placeholder || `Select ${field.displayName}`}
            triggerClass="w-full min-w-0"
            value={fieldValue(field, drafts, savedSecrets !== undefined)}
          />
        {:else}
          <TextInput
            aria-invalid={missing.includes(field.key)}
            aria-label={field.displayName}
            autocomplete={field.secret ? "off" : undefined}
            class="w-full"
            id={`${id}-${field.key}`}
            inputmode={field.valueKind === "integer" || field.valueKind === "decimal" ? "numeric" : undefined}
            oninput={(event) => onupdate(field.key, event.currentTarget.value)}
            placeholder={saved ? "Enter replacement" : field.placeholder || undefined}
            required={field.required}
            type={field.secret ? "password" : field.valueKind === "integer" ? "number" : "text"}
            value={fieldValue(field, drafts, savedSecrets !== undefined)}
          />
        {/if}
      </label>
    {/each}
  </div>
{/if}
