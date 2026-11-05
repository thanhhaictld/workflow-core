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
