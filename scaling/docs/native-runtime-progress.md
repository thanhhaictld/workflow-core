# Native Provisioner implementation and verification

The approved implementation preserves upstream authoring and execution semantics,
keeps Hub generic and retains existing sequential runs. Migration 012 is additive.

Implementation and verification checklist:

- [x] Native registration, snapshot codec, executor and typed activity adapter
- [x] Atomic fenced Hub commits, durable inbox, subscriptions, timers and status
- [x] Combined Provisioner, metadata publication and independently bounded pools
- [x] Native semantic scenarios and PostgreSQL races/recovery/coexistence
- [x] Deployment, monitoring and operational documentation
- [x] Release build, real PostgreSQL tests and affected monitor checks

Compatibility is a verification gate. Successful compilation does not establish
native parity. Upstream SubWorkflowStepBody is unsupported.

Verification on 4 November 2026:

- Release scaling solution build passed. The existing xUnit2031 warning in
  `HubGatewayTests` remains.
- All 58 tests in the full suite passed with `WFC_TEST_PG` pointing to PostgreSQL 17
  in an isolated database, using the .NET 10 Docker SDK alongside PostgreSQL.
  Native coverage includes executor comparisons, typed/scalar/dictionary activity
  exchange, null and typed events, boxed values, FIFO, fencing, event/timer races,
  cancellation, suspension, transport outages, and exhausted activity attempts.
  Workflow claims also filter registered definition/version capabilities during rollout.
  Initial pointer persistence is checked before any user step executes, including
  pointer identity retention after an uncommitted ordinary step attempt.
- `scripts/test-hub-native.ps1 -ManageHosts` passed: v1/v2 coexistence, exact v1
  result fields, native mappings, idempotent starts, durable timers, pointer and
  attempt history, and force-restart recovery of Hub and the combined Provisioner.
- Monitor type checks, all 10 topology tests, and the production build passed.
- Compose configuration, k6 JavaScript syntax, and both modified CI YAML files parsed.
- Two independent reviews found dispatch retention/cancellation, repeat-event,
  alias ambiguity, result-type, and completion-retry issues. These were corrected
  and covered by regression tests. No external review CLI was selected.

Live browser verification could not run. Automatic command review rejected both
the npm and explicit Node launchers for the local Next.js server with
`blocked by policy`, without a more detailed reason. No workaround bypassed that
restriction. Windows runs also exposed timing-sensitive existing direct-runtime
queue tests; the complete suite is verified inside the Docker environment alongside
PostgreSQL rather than treating database-free test returns as coverage.

See [native runtime usage and boundaries](hub-native-fluent-runtime.md).
