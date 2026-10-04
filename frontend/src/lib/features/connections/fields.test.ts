import assert from "node:assert/strict";
import { test } from "node:test";
import type { AdapterField, ConnectionInspection } from "../../api/connections.ts";
import {
  adapterValue,
  authenticationInput,
  configurationInput,
  fieldValue,
  inspectionDrafts,
  valueToDraft,
} from "./fields.ts";

const field: AdapterField = {
  key: "name",
  displayName: "Name",
  valueKind: "text",
  required: true,
  secret: false,
  defaultValue: null,
  options: [],
  placeholder: "",
};

test("authentication validates required fields and omits empty optional values", () => {
  assert.deepEqual(authenticationInput([field], { name: "  " }), { missing: ["name"] });
  assert.deepEqual(authenticationInput([field, { ...field, key: "optional", required: false }], { name: "Alex" }), {
    missing: [],
    value: [{ key: "name", value: { kind: "text", value: "Alex" } }],
  });
});

test("configuration omits blank saved secrets and never uses a secret default", () => {
  const secret = { ...field, key: "token", secret: true, defaultValue: { kind: "text" as const, value: "hidden" } };
  const inspection: ConnectionInspection = {
    fields: [field, secret],
    settings: [{ key: "name", value: { kind: "text", value: "Saved" } }],
    configuredSecretKeys: ["token"],
    fieldProblems: [],
    authenticationMethods: [],
  };
  assert.equal(fieldValue(secret, {}, true), "");
  assert.deepEqual(configurationInput(inspection, inspectionDrafts(inspection)), {
    missing: [],
    value: { settings: inspection.settings, secretPatches: [] },
  });
  assert.deepEqual(configurationInput({ ...inspection, configuredSecretKeys: [] }, { name: "Saved" }), {
    missing: ["token"],
  });
  assert.deepEqual(configurationInput(inspection, { name: "Saved", token: "new" }).value?.secretPatches, [
    { key: "token", value: [110, 101, 119] },
  ]);
});

test("conversion preserves wire kinds, UTF-8, false, and exact decimal text", () => {
  assert.deepEqual(adapterValue({ ...field, valueKind: "integer" }, "42"), { kind: "integer", value: 42 });
  assert.deepEqual(adapterValue({ ...field, valueKind: "timestamp" }, "123"), {
    kind: "timestampMilliseconds",
    value: 123,
  });
  assert.deepEqual(adapterValue({ ...field, valueKind: "boolean" }, "false"), { kind: "boolean", value: false });
  assert.deepEqual(adapterValue({ ...field, valueKind: "decimal" }, "1.2300"), { kind: "decimal", value: "1.2300" });
  assert.equal(valueToDraft(adapterValue({ ...field, valueKind: "bytes" }, "hello")), "hello");
  for (const valueKind of ["integer", "timestamp"] as const) {
    for (const raw of ["1.5", "NaN", "9007199254740992"]) {
      assert.throws(() => adapterValue({ ...field, valueKind }, raw), /Name must be an integer/);
    }
  }
});
