# Engineering archive and source index

The original `scaling/docs/` directory contains architecture decisions, investigations, experiment reports and CI references. **These remain in place** so source-code references, relative links and historical context do not break. This GitHub Pages site provides a reader-oriented structure without moving hundreds of linked diagrams and investigation artifacts.

The following is a curated map of the authoritative repository sources. Older investigations should not override the [current runtime](../architecture/index.md).

| Topic | Repository source |
| --- | --- |
| Hub runtime, candidate matching, failure recovery | [current-runtime.md](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/current-runtime.md) |
| Native C# registration and activity contracts | [hub-native-fluent-runtime.md](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/hub-native-fluent-runtime.md) |
| Workflow history and trace semantics | [execution-history.md](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/execution-history.md) |
| Scheduler/remote hosts | [hub-remote-scheduler-implementation.md](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/hub-remote-scheduler-implementation.md) |
| Admission and matching experiments | [hub-matching-claim-flow.md](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/hub-matching-claim-flow.md) |
| Native A–D SQL and receipt optimization | [native-commit-phased-optimization.md](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/native-commit-phased-optimization.md) |
| Single vs batch completion transaction experiment | [native-task-completion-batch-commit-experiment.md](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/native-task-completion-batch-commit-experiment.md) |
| PostgreSQL CPU and query analysis | [sql-cpu-statement-investigation.md](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/sql-cpu-statement-investigation.md) |
| Monitoring and dashboards | [operations-dashboard.md](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/operations-dashboard.md) |
| Production security/readiness | [production-readiness.md](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/production-readiness.md) |
| All historical docs, investigations and diagrams | [scaling/docs/ directory](https://github.com/thanhhaictld/workflow-core/tree/thanhhaictld/hub-memory-candidates-luna/scaling/docs) |

**Contribution guideline:** Put narrative, maintained product documentation in `docs/distributed/`, where it is searchable and navigable on GitHub Pages. Put raw benchmark evidence, dated investigations and working RFCs in `scaling/docs/`; link to them from a curated website page when relevant. Update both the page and the corresponding navigation entry in `mkdocs.yml`.
