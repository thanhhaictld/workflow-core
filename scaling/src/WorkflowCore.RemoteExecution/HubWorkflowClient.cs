using System.Text.Json;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Client;

namespace WorkflowCore.RemoteExecution;

/// <summary>Hub operations with native CLR value preservation for object-valued
/// workflow inputs and events. This never calls the legacy host/controller.</summary>
public sealed class HubWorkflowClient(HubRestClient client,HubSnapshotCodec codec)
{
    public async Task<Guid> StartAsync<TData>(string hub,string definition,int version,TData data,
        string? idempotencyKey=null,CancellationToken ct=default)
    {
        var response=await client.StartWorkflowAsync(new(hub,definition,version,codec.EncodeValue(data),idempotencyKey),ct);
        return response.GetProperty("workflowId").GetGuid();
    }
    public Task PublishEventAsync(string hub,string name,string key,object? data,string idempotencyKey,
        DateTime? effectiveAt=null,CancellationToken ct=default) =>
        client.PublishEventAsync(hub,new(name,key,codec.EncodeValue(data),idempotencyKey,effectiveAt),ct);
    public Task ChangeStatusAsync(Guid id,string status,string idempotencyKey,CancellationToken ct=default) =>
        client.ChangeStatusAsync(id,new(status,idempotencyKey),ct);
}
