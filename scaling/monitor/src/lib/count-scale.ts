export function countScale(values: Array<number | null | undefined>) {
  const present = values.filter((value): value is number =>
    typeof value === "number" && Number.isFinite(value) && value >= 0);
  if (!present.length) return null;

  const peak = Math.max(...present);
  const roughStep = Math.max(1, peak / 4);
  const magnitude = 10 ** Math.floor(Math.log10(roughStep));
  const normalized = roughStep / magnitude;
  const step = (normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 5 ? 5 : 10) * magnitude;
  const intervals = Math.max(1, Math.ceil(peak / step));
  const upper = intervals * step;
  return {
    peak, upper,
    ticks: Array.from({ length: intervals + 1 }, (_, i) => i * step),
    position: (value: number) => value / upper * 100
  };
}
