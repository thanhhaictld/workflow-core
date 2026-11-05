import { stringify } from "yaml";
import { diffLines } from "diff";
import type { DefinitionSchema } from "./definitions.ts";

// Alphabetical map keys give every snapshot field a stable order; arrays retain their original order.
export function definitionToYaml(schema: DefinitionSchema): string {
  return stringify(schema, { indent: 2, lineWidth: 0, sortMapEntries: true, aliasDuplicateObjects: false });
}

export interface DiffLine { text: string; number: number; kind: "same" | "added" | "removed" }
export interface DiffRow { left?: DiffLine; right?: DiffLine }

export function definitionDiff(base: string, target: string): DiffRow[] {
  const changes = diffLines(base, target);
  const rows: DiffRow[] = [];
  let leftNumber = 1, rightNumber = 1;
  let removed: DiffLine[] = [], added: DiffLine[] = [];
  function flush() {
    for (let i = 0; i < Math.max(removed.length, added.length); i++)
      rows.push({ left: removed[i], right: added[i] });
    removed = []; added = [];
  }
  for (const change of changes) {
    const lines = change.value.split("\n");
    if (lines.at(-1) === "") lines.pop();
    if (!change.added && !change.removed) flush();
    for (const text of lines) {
      if (change.removed) removed.push({ text, number: leftNumber++, kind: "removed" });
      else if (change.added) added.push({ text, number: rightNumber++, kind: "added" });
      else rows.push({ left: { text, number: leftNumber++, kind: "same" }, right: { text, number: rightNumber++, kind: "same" } });
    }
  }
  flush();
  return rows;
}
