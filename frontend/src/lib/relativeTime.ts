const relativeTimeFormatter = new Intl.RelativeTimeFormat(undefined, {
  numeric: "auto",
});

export function relativeObservation(value: string | null): string {
  if (!value) return "Never observed";
  const observedAt = Date.parse(value);
  if (!Number.isFinite(observedAt)) return "Unknown";
  const minutes = Math.round((observedAt - Date.now()) / 60_000);
  if (Math.abs(minutes) < 60) return relativeTimeFormatter.format(minutes, "minute");
  return relativeTimeFormatter.format(Math.round(minutes / 60), "hour");
}
