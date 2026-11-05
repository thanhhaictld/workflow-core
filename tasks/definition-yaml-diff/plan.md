# Plan: Workflow definition YAML viewer and version diff

Date: 2026-11-05. User authorized generating this plan and handing off implementation to GPT-6.1 Terra, high reasoning, in another worktree. The user explicitly approved using `fix/activity-policy-revisions` as the base after the default repository base proved to lack `scaling/monitor`.

## Outcome and ownership

Replace the definition detail page's field-by-field metadata comparison and step-card inspector with a read-only YAML document viewer and a familiar two-version text diff. Preserve the definition catalog, archived data, API contracts, runtime version selection, and same-version drift warnings.

Implementation owner: the receiving agent in `F:/Project/orca-workspaces/workflow-core/feat-definition-yaml-diff`, branch `thanhhaictld/feat-definition-yaml-diff`. Starting implementation baseline: `ba1d48134e74ce15afb1ee45cbc23c0b7b4882a4` from `fix/activity-policy-revisions`. The separate originating worktree must not be edited. Other agents may be working in this repository; do not revert their work or change shared services indiscriminately.

This plan and [todo.md](todo.md) are feature-specific because root `tasks/plan.md` and `tasks/todo.md` contain unfinished, unrelated Hub/Management work. Do not overwrite those files. User authorization covers implementation now; routine reversible implementation choices do not need another approval gate.

## Evidence and current behavior

- Read `AGENTS.md`, `scaling/monitor/README.md`, and `scaling/docs/definition-version-management.md` first. Follow applicable frontend, TypeScript, testing, and verification skills.
- `scaling/monitor/src/components/definition-detail.tsx` currently loads all archived versions, chooses previous/latest versions, calls the structural diff endpoint, displays change rows, and separately renders step cards/raw JSON.
- `scaling/monitor/src/components/definition-list.tsx` supplies the searchable catalog. Preserve navigation and update presentation copy only where needed.
- `scaling/monitor/src/lib/definitions.ts` defines the snapshot and comparison types. `src/lib/api.ts` supplies cancellation-aware GET requests through `/backend`.
- Backend metadata currently lives in `scaling/src/WorkflowCore.Hub.Management/DefinitionCatalog.cs`, not the older `scaling/monitor/api/DefinitionCatalog.cs` location. Existing routes are still mapped in `scaling/monitor/api/Program.cs` at this baseline. Inspect actual source before making changes; another workstream is extracting management endpoints.
- `GET /api/definitions/{id}/versions` already returns `CatalogVersion[]` with complete schemas. Reuse that data for YAML and text comparison. The backend structural diff endpoint can remain for existing clients and tests.
- The monitor uses Next 16, React 19, Tailwind 4, TypeScript, and Node's test runner via `npm run test:topology`. Check the package manifest and actual lockfile policy before installing dependencies.

## Product requirements

1. Keep `/definitions` as the definition catalog. Opening a definition offers clear `YAML` and `Compare versions` modes. Default to viewing the latest archived version; comparison defaults to previous -> latest when two or more exist.
2. YAML mode has a labeled version selector, syntax-highlighted read-only YAML, line numbers, sensible scrolling, and a Copy YAML action with success/failure feedback. A definition with a single version remains fully viewable.
3. Compare mode has labeled Base and Target version selectors. Compare only versions belonging to this definition. Show base on the left and target on the right, with clear version headings. Version order is user-selectable, including reverse comparisons.
4. Provide side-by-side and unified diff modes. Use synchronized vertical scrolling for split panes; use unified display on narrow screens while retaining an explicit view choice where practical. Changed lines have added/removed markers and useful line numbers; color alone must not convey change.
5. Replace the old metadata-path change table, Added/Removed/Modified filters, and step-card inspection as the primary UI. Do not duplicate both interaction models. Retain helpful version metadata and drift warnings in a compact surrounding layout.
6. Loading, backend failures, missing definitions/snapshots, no available versions, same-version selection, and no-content-change states must be distinct. A failed refresh must not make stale data look current. Clear an old definition's selection/content when route identity changes; stale asynchronous responses must not overwrite current state.
7. Explain concisely that YAML describes archived metadata generated from C#. It omits executable expressions, closures, and step-body implementation; a matching diff is not proof of equivalent workflow behavior. Same-version drift means the archive can differ from the currently registered C# metadata.

## Data and rendering decisions

- Generate YAML in the frontend from existing archived `DefinitionSchema` objects. Keep JSON storage, schema format, hash calculation, and APIs unchanged. Do not add YAML database columns or a backend YAML endpoint.
- Create one pure `definitionToYaml`-style helper used by viewer, diff, and clipboard. Work from explicit typed snapshot fields and a documented stable key order. Preserve all available schema fields, including nulls, empty arrays, retry/cancellation/compensation data, IDs, and expression-binding descriptors. Exclude `CatalogVersion` envelope timestamps/hash/drift from the document; show these in surrounding UI.
- Use fixed two-space indentation, LF newlines, a consistent trailing newline, safe quoting, and a deterministic serializer configuration. Preserve meaningful array ordering and numeric transition IDs. Do not rewrite steps into a new DSL, infer missing expressions, replace nulls with invented values, or normalize away actual changes.
- Prefer a maintained YAML serializer and diff utility over implementing YAML escaping or an edit-distance algorithm by hand. First inspect installed/repository dependencies. Small fit-for-purpose dependencies are authorized when needed; verify their current official documentation, license, browser/SSR support, and React compatibility. Avoid an editor suite or remote CDN workers solely for read-only rendering. Keep highlighting lightweight and render all metadata as escaped text, never trusted HTML.
- Add isolated presentation components/helpers as needed, for example `definition-yaml.ts`, `definition-yaml-viewer.tsx`, and `definition-yaml-diff.tsx`. Names are proposals, not a requirement to add layers. Keep data fetching/selection in the detail component and pure serialization/diff logic independently testable.
- Both versions include their actual `version` field. Consequently two versions with otherwise identical snapshots still show a version-line change. Do not claim the full YAML is identical in that case; an optional message can say only the version differs. Same-version selection can show an instruction to choose distinct versions. Line counts, if shown, describe lines, not semantic change counts.
- Text diff does not preserve the old ExternalId-based semantic matching. Numeric step ID shifts can cause multiple changed lines; disclose this limitation briefly without claiming a structural or behavioral compatibility analysis.

## Scope boundaries

May edit: definition UI and its focused helpers/tests, monitor package manifest/lockfile if needed, `scaling/monitor/README.md`, `scaling/docs/definition-version-management.md`, and this feature's plan/checklist/report. Adjust shared UI/styles only if the change is small and necessary; preserve other pages.

Out of scope: workflow editing/import/publish/deployment/default-version controls, changing executable definitions, Hub scheduling/processor/provisioner behavior, management extraction, migrations, hash semantics, authentication redesign, upstream engine code, or deleting the structural diff API/types merely because the new UI stops using them. No database reset, merge to another branch, push, or PR is required by this handoff.

## Ordered implementation slices

Detailed criteria and checks are in [todo.md](todo.md).

1. Inspect the actual baseline and choose serializer/diff/rendering approach. Record only decisions needed to implement; do not start unrelated refactors.
2. Deliver latest/single-version YAML viewing with stable serialization and clipboard feedback. Add focused behavioral tests for serialization fidelity and determinism.
3. Deliver two-version text comparison with split/unified rendering, line markers/numbers, and synchronized scrolling. Reuse exactly the same YAML strings as the viewer. Add focused diff behavior checks.
4. Complete selection, refresh, error, narrow-screen, keyboard, and dark-mode behavior; remove obsolete detail-page UI and imports. Verify in a real browser against available backend snapshots.
5. Update docs, run required checks, inspect the final diff, and write the implementation report with exact evidence and any blockers.

Checkpoints: after slice 2, latest and single-version viewing must work; after slice 3, v1/v2 must show the added ConfigureAccessStep with surrounding context; after slice 4, mode/version switches and failure states must stay honest; after slice 5, tests/build/docs and a user-visible verification must support completion.

## Verification matrix

Focused automated checks should cover observable results rather than component internals:

- YAML parses back to the same snapshot data, including all optional/null fields, nested arrays, mappings, branches, retry policies, booleans, and ID references.
- Strings resembling booleans, numbers, dates, YAML tags/aliases, keys with punctuation, colon/hash characters, multiline strings, Unicode, and literal HTML are preserved as data. Equivalent objects with different property insertion orders yield identical YAML. Source snapshots are not mutated.
- A real v1/v2-shaped comparison shows the added step and transition changes; reversals swap additions/removals. Same input yields no textual changes; version-only differences stay visible; empty and long content remain usable.
- UI checks cover zero/one/multiple versions, copy success and denied clipboard access, same-version selection, refresh/rapid selection/route changes, backend errors, drift warnings, keyboard access, light/dark themes, and narrow/wide screens. Keep test fixtures in tests only; production must use real backend data.

Run from `scaling/monitor` with Node 22+:

```powershell
npm run check-types
npm run test:topology
npm run build
```

Add a focused npm script only if useful; document its exact invocation. Existing `src/lib/*.test.mjs` tests can import TS helpers through Node's stripping support. Browser verification is required for changed rendering: use available browser tooling, read its skill first, capture the observed page/results, and check browser console errors. Do not claim mocks prove live backend integration.

For real archived snapshots, use the existing `tenant-onboarding-versioned` v1/v2 sample and local API. Inspect `scripts/dev.ps1 help` and port/configuration options before starting services; avoid existing services' ports and do not stop unrelated processes. Existing API URL is configured via `WORKFLOW_API_URL`; browser requests use `/backend`.

If backend contracts change despite the intended frontend-only scope, explain why and run `dotnet build scaling/WorkflowCore.Scaling.slnx -c Release` plus relevant database integration checks with a configured schema and `WFC_TEST_PG`. A test that returns early without that variable is not database verification. `scripts/test-definition-management.ps1` also exercises workflow starts and instance writes and requires a running development stack plus management-write configuration; it is not a prerequisite for a frontend-only rendering change.

## Risks and resolutions

| Risk | Required response |
| --- | --- |
| YAML serialization loses/coerces values | Round-trip representative schemas using a real YAML parser and explicit stable formatting. |
| Two diff engines disagree | Generate YAML once; use one text-diff result for split and unified presentation. |
| New dependencies fail SSR or fetch CDN assets | Verify local bundling/build and browser behavior; prefer smaller local utilities. |
| Stale content after selections or failures | Cancel obsolete requests and tie displayed data/errors to the current identity and selection. |
| Large definitions freeze or overflow the page | Exercise a larger test document, use appropriate overflow and bounded computation; avoid premature infrastructure. |
| Parallel Management refactor changes routes/files | Trust current checked-out source, preserve public contracts, do not copy the older backend location from historical discussion. |
| Backend unavailable | Still implement/test pure and browser-rendered behavior; report live integration as blocked with prerequisites, never fabricate snapshots in production. |

## Completion report

Create `tasks/definition-yaml-diff/report.md` with what changed, files, dependency decisions, exact test/build/browser results, screenshots/artifact paths when available, limitations, and any remaining blockers. Update the checklist honestly. Review the diff for unrelated edits and unexpected data/API changes. Commits may contain only this feature and its planning artifacts; do not push or merge without a new instruction. Return a concise completion summary to the user in the receiving agent session.
