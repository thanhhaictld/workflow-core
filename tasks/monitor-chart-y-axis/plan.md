# Plan: readable count axes on monitor chart cards

## Goal

Operators must be able to estimate the number of workflows or activities from a
bar's height without hovering. The user's marked screenshot highlights Runnable
workflows, Ready activities, and Workflow completions on the operations overview.

## Evidence and scope

- Baseline: 813fee81f1e6d6c0fe5a9f870333f324bd66afcd, also the source checkout's HEAD.
- `scaling/monitor/src/components/operations-dashboard.tsx` contains `ChartBars`.
  It renders bars normalized to `Math.max(1, ...present)`, time endpoints, and a
  Peak footer, but no Y-axis ticks or horizontal guides.
- The same component also renders Activity completion events, Active leases,
  Workflows started, and Workflows failed. Apply the shared fix consistently.
- Existing zero values currently receive a minimum visible bar height, and the
  scale minimum of one is reported as Peak even when the actual peak is zero.
  Correct these count-reading inaccuracies as part of the chart fix.
- Read AGENTS.md and scaling/monitor/README.md before implementation.
- Planning files are scoped here to preserve the unrelated unfinished root plan.

## Implementation decisions

1. Keep the current card styling and shared component. Add a left count axis with
   a visible zero baseline and a small set of readable nonnegative integer ticks,
   plus subtle horizontal gridlines aligned with those ticks. Label the unit as
   Count or make the count meaning equally clear and accessible.
2. Use one scale calculation for tick placement and bar heights. Choose a sensible
   rounded upper bound at or above the actual maximum. Avoid duplicate tick labels
   for small counts. An axis for counts must not introduce fractional item ticks.
3. Keep the actual sample maximum separate from the scale upper bound. Peak, if
   retained, must show the actual peak: zero for all-zero samples, never a fabricated
   minimum of one. Known zeros have zero height; null remains visibly missing.
4. Preserve bucket ordering, time labels, exact timestamp/count hover information,
   existing colors, filters, refresh, and backend failure/empty states. If labels
   abbreviate large counts, retain exact values in hover/accessibility information.
5. Keep labels and plot aligned at desktop and narrow card widths, with room for
   large tick labels and no clipping or horizontal overflow. Maintain readable
   contrast in light and dark themes. Stay within the existing dependency set.

## Ordered tasks

1. Inspect current rendering and define scale behavior for no samples, all-zero,
   single-item, repeated, sparse/null, and large-count series. Implement the shared
   count scale and chart layout as one focused change.
2. Add focused behavior checks where scale calculations warrant them, following
   the existing Node test conventions. Verify count placement, integer/unique
   ticks, upper bound, true peak, and missing-versus-zero behavior. Do not build a
   new testing framework for this fix.
3. Run monitor checks and inspect rendered charts in a real browser. Save evidence
   and report the resulting files, checks, and any limitations. Update todo.md.

## Acceptance and verification

- All seven shared chart cards display a readable numeric count axis and aligned
  guides; bars accurately use that axis. For example, a sample peak of 65 must
  plot against count ticks enclosing 65, and a peak of 1,000 must be readable.
- Zero, one, sparse/null, empty, and large-count cases render honestly without
  NaN, duplicate tick labels, fake positive zero bars, or missing-data substitution.
- At a desktop three-column layout and a narrow/mobile layout, count labels,
  bars, and times remain legible. Light and dark themes are readable.
- From scaling/monitor: npm run check-types; npm run test:topology; npm run build.
- Inspect /operations (confirm actual route in code) in a real browser. Use an
  available live backend for the user's scenario. Controlled fixtures are allowed
  only in tests/isolated verification, never as a production fallback. Capture
  screenshots and explain any live-backend prerequisite that blocks verification.
- Review the final diff for scope and correctness. Do not merge or deploy.

## Ownership and constraints

The receiving GPT-6.1 Terra agent owns frontend implementation, minimal relevant
tests/helpers, and these task documents in this worktree. It is not alone in the
repository: preserve others' work and do not edit or revert other worktrees.
Backend contracts, SQL, runtime, scheduler, and workflow execution are outside
scope. Never reset the database or invent monitoring values. No additional human
approval is needed for this already requested reversible implementation.

## Handoff

- Model: gpt-6.1-terra; reasoning effort: high.
- Worktree: F:/Project/orca-workspaces/workflow-core/fix-monitor-chart-y-axis.
- Branch: thanhhaictld/fix-monitor-chart-y-axis.
- Screenshot source: C:/Users/haido-ws/AppData/Local/Temp/orca-paste-1793849360807-cd63905b-eff1-467c-8819-e6c6373ebfad.png.
- Full ownership handoff: implement and verify independently; report completion
  in the receiving agent session. The original agent will not supervise a Run.
