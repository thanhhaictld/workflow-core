# Workflow Core documentation

Welcome to the **Workflow Core** documentation site. This repository includes the original embeddable workflow engine **and** a separately hosted **Distributed Workflow Hub** runtime. Choose the runtime you want to build.

| | Distributed Workflow Hub | Original Workflow Core |
| --- | --- | --- |
| Best for | Durable distributed workflow execution, REST/SSE processors, Hub scale-out and PostgreSQL-authoritative matching | Embeddable workflow execution within a .NET application |
| Where code lives | `scaling/` | `src/`, `test/` |
| How it starts | Hub Gateway plus separate native Provisioner / processors | `AddWorkflow`, `IWorkflowHost` |
| First tutorial | [Day 1: first durable workflow](distributed/tutorials/day-1-first-workflow.md) | [Original getting started](getting-started.md) |
| Scale-out tutorial | [Day 2: two Hub replicas](distributed/tutorials/day-2-multi-hub.md) | [Multi-node clusters](multi-node-clusters.md) |

## Learn Distributed Workflow Hub

[Start with the two-day learning path](distributed/learning-path.md){ .md-button .md-button--primary }
[Explore Hub architecture](distributed/architecture/index.md){ .md-button }

```mermaid
flowchart LR
    A["Day 1: Hub + PostgreSQL + .NET app"] --> B["Day 2: two Hubs + processors"]
    B --> C["Throughput / latency A-B benchmark"]
    C --> D["PostgreSQL HA and scaling choices"]
```

!!! important "Different runtimes, different instructions"
    The Hub-native processors do not connect to PostgreSQL and should not start the original `IWorkflowHost` poller. The original Workflow Core docs remain available in the **Original Workflow Core** navigation section. Historical `scaling/docs/` investigations remain in the [engineering archive](distributed/reference/engineering-archive.md).

## Useful paths

- [Distributed Hub overview](distributed/index.md)
- [Reference configuration](distributed/reference/configuration.md)
- [Run locally and monitor](distributed/operations/deployment.md)
- [Publish with GitHub Pages](distributed/operations/github-pages.md)
- [Source repository](https://github.com/thanhhaictld/workflow-core)
