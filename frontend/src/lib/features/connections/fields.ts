import type {
  AdapterField,
  AdapterSetting,
  AdapterValue,
  AdapterValueDisplay,
  ConnectionInspection,
  ReconfigureConnectionInput,
} from "../../api/connections.ts";

export type FieldDrafts = Record<string, string>;
export type FieldResult<Value> = { value: Value; missing: string[] } | { value?: undefined; missing: string[] };

export function valueToDraft(value: AdapterValue | AdapterValueDisplay | null | undefined): string | undefined {
  if (!value) return;
  if (value.kind === "bytes") return new TextDecoder().decode(Uint8Array.from(value.value));
  return String(value.value);
}

export function fieldValue(field: AdapterField, drafts: FieldDrafts, configuration = false): string {
  return drafts[field.key] ?? (configuration && field.secret ? "" : (valueToDraft(field.defaultValue) ?? ""));
}

export function fieldItems(field: AdapterField) {
  if (field.options.length > 0) {
    return field.options.flatMap((option) => {
      const value = valueToDraft(option.value);
      return value === undefined ? [] : [{ value, label: option.displayName }];
    });
  }
  return field.valueKind === "boolean"
    ? [
        { value: "true", label: "Yes" },
        { value: "false", label: "No" },
      ]
    : [];
}

export function adapterValue(field: AdapterField, raw: string): AdapterValue {
  switch (field.valueKind) {
    case "integer":
    case "timestamp": {
      const value = Number(raw);
      if (!Number.isSafeInteger(value)) throw new Error(`${field.displayName} must be an integer`);
      return { kind: field.valueKind === "integer" ? "integer" : "timestampMilliseconds", value };
    }
    case "boolean":
      return { kind: "boolean", value: raw === "true" };
    case "bytes":
      return { kind: "bytes", value: Array.from(new TextEncoder().encode(raw)) };
    case "decimal":
      return { kind: "decimal", value: raw };
    default:
      return { kind: "text", value: raw };
  }
}

export function authenticationInput(fields: AdapterField[], drafts: FieldDrafts): FieldResult<AdapterSetting[]> {
  const missing = fields
    .filter((field) => field.required && !fieldValue(field, drafts).trim())
    .map((field) => field.key);
  if (missing.length) return { missing };
  return {
    missing,
    value: fields.flatMap((field) => {
      const raw = fieldValue(field, drafts);
      return raw === "" && !field.required ? [] : [{ key: field.key, value: adapterValue(field, raw) }];
    }),
  };
}

export function configurationInput(
  inspection: ConnectionInspection,
  drafts: FieldDrafts,
): FieldResult<ReconfigureConnectionInput> {
  const missing = inspection.fields
    .filter(
      (field) =>
        field.required &&
        !fieldValue(field, drafts, true).trim() &&
        !(field.secret && inspection.configuredSecretKeys.includes(field.key)),
    )
    .map((field) => field.key);
  if (missing.length) return { missing };
  return {
    missing,
    value: {
      settings: inspection.fields.flatMap((field) => {
        if (field.secret) return [];
        const raw = fieldValue(field, drafts, true);
        return raw === "" && !field.required ? [] : [{ key: field.key, value: adapterValue(field, raw) }];
      }),
      secretPatches: inspection.fields.flatMap((field) => {
        if (!field.secret) return [];
        const raw = fieldValue(field, drafts, true);
        return raw === "" ? [] : [{ key: field.key, value: Array.from(new TextEncoder().encode(raw)) }];
      }),
    },
  };
}

export function inspectionDrafts(inspection: ConnectionInspection): FieldDrafts {
  return Object.fromEntries(inspection.settings.map((setting) => [setting.key, valueToDraft(setting.value) ?? ""]));
}
