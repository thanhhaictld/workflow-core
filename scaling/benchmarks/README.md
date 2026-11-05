# Distributed workflow API benchmark

This is an HTTP facade over the NEW PostgreSQL runtime. It does not start the
original Workflow Core poller or consumers.

### Run

Create a PostgreSQL database, apply the original Workflow Core EF migrations
and the three `scaling/sql/002..004` scripts. For a new database, the EF
migrations may be invoked from the existing provider tooling or a dedicated
migration job.

```bash
export WFC_PG='Host=localhost;Database=workflow;Username=workflow;Password=***'
dotnet run --project scaling/monitor/api --urls http://localhost:5180

# second terminal:
k6 run scaling/benchmarks/k6-start-workflows.js -e URL=http://localhost:5180 -e RATE=100
```

Use `GET /api/workflows/{id}` to inspect persisted status, and monitor
`wfc_scale.work_item` for backlog and completion. k6 only measures HTTP
ingestion, NOT background workflow completion. For throughput analysis, record
database work-item completion count, queue lag, and worker CPU concurrently.

**Security:** This is a development benchmark API with no authentication.
Do not expose it to untrusted networks or use it with production credentials.
