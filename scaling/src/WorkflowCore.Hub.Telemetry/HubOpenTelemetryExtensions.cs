using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Logs;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace WorkflowCore.Hub.Telemetry;

/// <summary>
/// OTLP/HTTP telemetry for the standalone Hub and remote workers.
/// Disabled unless OTEL_EXPORTER_OTLP_ENDPOINT is explicitly configured.
/// </summary>
public static class HubOpenTelemetryExtensions
{
    public static IServiceCollection AddWorkflowHubOpenTelemetry(
        this IServiceCollection services, string serviceName, bool httpServer = false)
    {
        var endpoint = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT");
        if (string.IsNullOrWhiteSpace(endpoint))
            return services;

        if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) ||
            uri.Scheme is not ("http" or "https"))
            throw new InvalidOperationException(
                "OTEL_EXPORTER_OTLP_ENDPOINT must be an absolute HTTP(S) URL.");

        var baseEndpoint = endpoint.TrimEnd('/');
        var instance = Environment.GetEnvironmentVariable("OTEL_SERVICE_INSTANCE_ID");
        if (string.IsNullOrWhiteSpace(instance))
            instance = $"{Environment.MachineName}-{Guid.NewGuid():N}";

        var otel = services.AddOpenTelemetry()
            .ConfigureResource(resource => resource
                .AddService(serviceName, serviceVersion: "1.0.0", serviceInstanceId: instance))
            .WithMetrics(metrics =>
            {
                metrics.AddMeter(HubTelemetry.MeterName, "WorkflowCore.Hub.Process",
                        "WorkflowCore.Distributed", "Npgsql")
                    .AddRuntimeInstrumentation()
                    .AddHttpClientInstrumentation();

                if (httpServer)
                    metrics.AddAspNetCoreInstrumentation();

                // P0: default OTEL histogram boundaries are too coarse to
                // distinguish 1ms from 10ms PostgreSQL lock/claim waits.
                // Fixed sub-ms-to-minute buckets retain accurate tail estimates
                // without introducing high-cardinality series.
                var latencyBuckets = new ExplicitBucketHistogramConfiguration
                {
                    Boundaries = new double[] {
                        0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025,
                        0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60
                    }
                };
                metrics.AddView("workflow_hub_native_event_lane_lock_wait_seconds", latencyBuckets);
                metrics.AddView("workflow_hub_claim_duration_seconds", latencyBuckets);
                metrics.AddView("workflow_hub_native_commit_duration_seconds", latencyBuckets);
                metrics.AddView("workflow_hub_native_workflow_row_lock_duration_seconds", latencyBuckets);
                metrics.AddView("workflow_hub_native_history_write_duration_seconds", latencyBuckets);
                foreach (var instrument in new[] {
                    "workflow_hub_worker_body_duration_seconds",
                    "workflow_hub_worker_completion_http_duration_seconds",
                    "workflow_hub_activity_completion_duration_seconds",
                    "workflow_hub_activity_db_pool_wait_seconds",
                    "workflow_hub_activity_workflow_lock_seconds",
                    "workflow_hub_activity_task_lock_seconds",
                    "workflow_hub_activity_db_commit_seconds",
                    "workflow_hub_claim_db_pool_wait_seconds",
                    "workflow_hub_claim_candidate_query_seconds",
                    "workflow_hub_claim_permit_lock_seconds",
                    "workflow_hub_claim_permit_check_seconds"
                })
                    metrics.AddView(instrument, latencyBuckets);

                metrics.AddOtlpExporter(options =>
                {
                    options.Protocol = OtlpExportProtocol.HttpProtobuf;
                    options.Endpoint = new Uri(baseEndpoint + "/v1/metrics");
                });
            })
            .WithTracing(tracing =>
            {
                tracing.AddSource(HubTelemetry.ActivitySourceName, "WorkflowCore.Distributed")
                    .AddHttpClientInstrumentation();
                // SQL tracing is opt-in: queries can reveal schema or business data,
                // and high-volume DB spans perturb normal benchmarks.
                if (Environment.GetEnvironmentVariable("WFC_OTEL_SQL_SPANS_ENABLED") == "true")
                    tracing.AddSource("Npgsql");

                if (httpServer)
                    tracing.AddAspNetCoreInstrumentation(options =>
                    {
                        // Avoid traces spanning days for SSE, plus noisy health
                        // and diagnostic polling. Business endpoints are traced.
                        options.Filter = ctx =>
                        {
                            var path = ctx.Request.Path.Value ?? "";
                            return !path.StartsWith("/health/", StringComparison.Ordinal) &&
                                !path.EndsWith("/workers/events", StringComparison.Ordinal) &&
                                !path.EndsWith("/diagnostics", StringComparison.Ordinal);
                        };
                    });

                tracing.AddOtlpExporter(options =>
                {
                    options.Protocol = OtlpExportProtocol.HttpProtobuf;
                    options.Endpoint = new Uri(baseEndpoint + "/v1/traces");
                });
            });

        // Logs are exported only when explicitly opted in. Do not log workflow
        // input/output payloads, auth headers or tenant identifiers.
        if (Environment.GetEnvironmentVariable("WFC_OTEL_LOGS_ENABLED") == "true")
        {
            services.AddLogging(logging => logging.AddOpenTelemetry(options =>
            {
                options.IncludeScopes = true;
                options.IncludeFormattedMessage = false;
                options.ParseStateValues = true;
                options.AddOtlpExporter(exporter =>
                {
                    exporter.Protocol = OtlpExportProtocol.HttpProtobuf;
                    exporter.Endpoint = new Uri(baseEndpoint + "/v1/logs");
                });
            }));
        }

        return services;
    }
}
