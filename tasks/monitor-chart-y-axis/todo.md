# Monitor chart count-axis checklist

Plan: [plan.md](plan.md). Scope: shared ChartBars in operations-dashboard.tsx.

## 1. Implement shared count scale and layout

- [x] Inspect current chart component, data types, formatters, and project guidance.
- [x] Add integer count ticks, zero baseline, aligned gridlines, and matching bar heights.
- [x] Keep actual peak separate from axis maximum; render zero and missing values honestly.
- [x] Preserve colors, exact value/time information, and card/filter/refresh behavior.

Acceptance: all seven shared chart cards have readable accurate count scales.
Verification: focused rendering inspection and scale edge cases.
Dependencies: none. Likely files: operations-dashboard.tsx and a small scale helper
only if useful. Estimated scope: 1-2 implementation files.

## 2. Verify count behavior

- [x] Cover zero, one, 65, 1,000, sparse/null, no-sample, and large-count scenarios.
- [x] Add focused behavior tests when needed using existing test conventions.
- [x] Run npm run check-types and npm run test:topology from scaling/monitor.

Acceptance: no fractional/duplicate item ticks, misleading peak, fake positive
zero bars, or missing-data substitution. Verification: focused tests and rendered
edge-case inspection. Dependencies: task 1. Likely files: helper/test alongside
existing lib tests; operations-dashboard.tsx. Estimated scope: 1-3 files.

## 3. Browser and delivery checkpoint

- [x] Run npm run build from scaling/monitor.
- [x] Inspect actual operations charts at desktop and narrow widths in light/dark themes.
- [x] Save screenshots/evidence and report any blocked check with its prerequisite.
- [x] Review the final diff; summarize implementation, validation, and remaining limitations.
- [x] Update this checklist with actual results; leave the fix ready for review without merging/deploying.

Acceptance: readable axes and aligned plot/time labels without clipping or
overflow; original monitoring states preserved. Verification: real browser and
build output. Dependencies: tasks 1-2. Likely files: these task documents and
verification evidence only. Estimated scope: 1-3 files.

## Results (2026-11-05)

- Shared count scale and axes apply to all seven charts. Focused Node tests cover empty, null, zero, one, 65, 1,000, and 1,234,567,890, including tick uniqueness and positions.
- `npm run check-types`, `npm run test:topology` (12 passed), and `npm run build` passed. Node emits the existing module-type warning during tests. Dependencies were installed with `npm install --no-package-lock` because this monitor has no lockfile.
- Inspected real browser rendering with an isolated intercepted API fixture on `/operations`, `/operations/hub`, and `/operations/history`. Desktop light and dark and 390px dark were inspected; 390px layouts had no horizontal document overflow. Screenshots: `desktop-light.png`, `desktop-hub-dark.png`, `mobile-dark.png`, and `mobile-history-dark.png`. Fixture values are not application data.
- Live-data chart inspection was blocked: this worktree's monitor server has no `WORKFLOW_HUB_URL` or `WORKFLOW_HUB_TOKEN`; the unconfigured state correctly reported monitoring unavailable. A configured Hub with migration 011 and persisted samples is required to inspect live charts.
- Reviewed diff and whitespace; no merge or deployment performed.
