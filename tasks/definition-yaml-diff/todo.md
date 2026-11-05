# YAML definition review implementation checklist

Plan: [plan.md](plan.md). Implementation is authorized by the user's handoff request. Check items only when evidence exists. Do not alter unrelated root task files.

## 1. Confirm baseline and rendering choices

Dependencies: none. Likely files: package manifest/lockfile, this plan. Scope: small.

- [x] Read repository guidance and inspect detail page, snapshot types, current metadata contracts, test conventions, and dependency policy.
- [x] Confirm this checkout starts from approved `fix/activity-policy-revisions` commit `ba1d48134e74ce15afb1ee45cbc23c0b7b4882a4`; preserve other worktrees.
- [x] Choose local YAML serialization, diff computation, and syntax highlighting with official-documentation verification for new dependencies; record necessary rationale.

Verification: record installed Node/npm versions and baseline/prerequisite status. Do not broaden into runtime or endpoint extraction.

## 2. View an archived version as YAML

Dependencies: 1. Likely files: `src/lib/definition-yaml.ts`, focused `.test.mjs`, `definition-yaml-viewer.tsx`, `definition-detail.tsx`. Scope: medium.

- [x] Deterministic serialization includes all schema data, preserves array order and scalar types, and excludes catalog capture metadata.
- [x] Latest and user-selected versions render highlighted read-only YAML with line numbers, scrolling, and Copy YAML feedback; a single version works independently of comparison.
- [x] Focused tests prove fidelity/round-trip, stable property order, safe scalar handling, and non-mutation.

Verification: focused tests and type check pass; inspect a rendered latest/single-version document. Checkpoint: YAML viewing is a complete usable slice.

## 3. Compare two versions as text

Dependencies: 2. Likely files: `definition-yaml-diff.tsx`, focused diff helper/tests if needed, `definition-detail.tsx`. Scope: medium.

- [x] Base/target selectors default previous -> latest and support any two archived versions of this definition; compared text matches viewer/clipboard text.
- [x] Split and unified views show coherent aligned changes, surrounding context, meaningful line numbers and non-color markers; split panes scroll together.
- [ ] Focused checks cover added step/transition changes, reverse comparison, identical text, and version-only changes without semantic-compatibility claims.

Verification: focused tests and type check pass; view v1/v2 and reverse it in a browser. Checkpoint: ConfigureAccessStep insertion is understandable in context.

## 4. Complete interactions and replace obsolete presentation

Dependencies: 3. Likely files: detail/viewer/diff components, catalog copy, minimal shared style only if necessary. Scope: medium.

- [x] Remove old detail-page change table, change filters, and step cards; keep catalog navigation, version provenance, and drift warnings.
- [ ] Verify empty/single/same-version/missing/error states, loading/refresh, rapid selections/route changes, and clipboard failure without invented or mislabeled stale data.
- [ ] Verify keyboard focus/labels, light/dark styling, narrow unified and wide split layout, long lines/documents, and concise metadata limitations.

Verification: real-browser checks and console inspection with results recorded. Checkpoint: changed routes and failure states remain accurate and usable.

## 5. Document, verify, and deliver

Dependencies: 4. Likely files: monitor README, definition-version-management doc, this checklist, report. Scope: medium.

- [x] Update docs for YAML viewing, text comparison, same-version drift, omitted executable logic, and numeric step ID noise; retain API/archive contract descriptions.
- [x] Run `npm run check-types`, `npm run test:topology`, `npm run build`, and any new focused command from `scaling/monitor`; report failures and prerequisites accurately.
- [x] Review the final diff against the approved source baseline for unrelated changes, archive/API mutations, test suppression, or fake production data; write `report.md` with evidence and remaining limitations.

Verification: checklist reflects actual results, browser evidence is recorded, and implementation summary is delivered in the receiving agent session. Do not push, merge, reset a database, or stop unrelated services.

Unchecked items retain verification gaps; see [report.md](report.md).
