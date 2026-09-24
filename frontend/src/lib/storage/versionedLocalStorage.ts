export type ScopedLocalStorageKey = `dev.trakkin:${string}:${string}`;

export type VersionedLocalStorageDefinition<T> = {
  name: string;
  version: number;
  decode: (value: unknown) => T | undefined;
};

export type VersionedLocalStorageValue<T> = {
  readonly key: ScopedLocalStorageKey;
  readonly version: number;
  read: () => T | undefined;
  write: (value: T) => boolean;
  clear: () => void;
  subscribe: (listener: (value: T | undefined) => void) => () => void;
};

type StoredEnvelope = {
  version: number;
  value: unknown;
};

const applicationStorageNamespace = "dev.trakkin";
const basePathInstanceId = import.meta.env.BASE_URL.replace(/^\/+|\/+$/g, "") || "root";
const localStorageScope = encodeURIComponent(basePathInstanceId);

export function defineVersionedLocalStorage<T>(
  definition: VersionedLocalStorageDefinition<T>,
): VersionedLocalStorageValue<T> {
  validateDefinition(definition);
  const key = storageKey(definition.name);

  function read(): T | undefined {
    const raw = readStorageValue(key);
    if (raw === null) return undefined;
    const envelope = parseEnvelope(raw);
    if (!envelope || envelope.version !== definition.version) {
      removeStorageValue(key);
      return undefined;
    }
    const decoded = definition.decode(envelope.value);
    if (decoded === undefined) {
      removeStorageValue(key);
      return undefined;
    }
    return decoded;
  }

  function write(value: T): boolean {
    const decoded = definition.decode(value);
    if (decoded === undefined) throw new TypeError(`Invalid value for local storage key ${key}`);
    try {
      localStorage.setItem(key, JSON.stringify({ version: definition.version, value: decoded }));
      return true;
    } catch {
      return false;
    }
  }

  function clear(): void {
    removeStorageValue(key);
  }

  function subscribe(listener: (value: T | undefined) => void): () => void {
    if (typeof window === "undefined") return () => {};
    const handleStorage = (event: StorageEvent) => {
      if (event.storageArea !== window.localStorage || (event.key !== null && event.key !== key)) return;
      listener(read());
    };
    window.addEventListener("storage", handleStorage);
    return () => window.removeEventListener("storage", handleStorage);
  }

  return { key, version: definition.version, read, write, clear, subscribe };
}

function storageKey(name: string): ScopedLocalStorageKey {
  return `${applicationStorageNamespace}:${localStorageScope}:${encodeURIComponent(name.trim())}`;
}

function validateDefinition<T>(definition: VersionedLocalStorageDefinition<T>): void {
  if (!definition.name.trim()) throw new TypeError("Local storage value name cannot be empty");
  if (!Number.isSafeInteger(definition.version) || definition.version < 1) {
    throw new TypeError(`Local storage version must be a positive integer for ${definition.name}`);
  }
}

function readStorageValue(key: ScopedLocalStorageKey): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function removeStorageValue(key: ScopedLocalStorageKey): void {
  try {
    localStorage.removeItem(key);
  } catch {}
}

function parseEnvelope(raw: string): StoredEnvelope | undefined {
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!isRecord(parsed) || !Number.isSafeInteger(parsed.version) || !("value" in parsed)) return undefined;
    return { version: parsed.version as number, value: parsed.value };
  } catch {
    return undefined;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
