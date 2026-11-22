using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using WorkflowCore.Hub;

namespace WorkflowCore.RemoteExecution;

/// <summary>Records offered credits before sending them, including across a processor restart.</summary>
internal sealed class HubPushCreditJournal : IDisposable
{
    internal sealed record Offer(string OwnerUrl, HubPushGrantRecovery Recovery);
    internal sealed record Snapshot(Guid ProcessorId, Offer[] Offers);

    private readonly string _path;
    private readonly FileStream _lock;
    public Snapshot Initial { get; }

    public HubPushCreditJournal(HubRuntimeOptions options)
    {
        var scope = $"{Environment.MachineName}|{options.HubUrl.TrimEnd('/')}|{options.WorkerHub}|" +
            System.Reflection.Assembly.GetEntryAssembly()?.GetName().Name;
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(scope)));
        _path = Path.GetFullPath(options.PushCreditJournalPath ??
            Path.Combine(Path.GetTempPath(), $"wfc-hub-push-{hash}.json"));
        Directory.CreateDirectory(Path.GetDirectoryName(_path)!);
        _lock = new FileStream(_path + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        try
        {
            if (File.Exists(_path))
                Initial = JsonSerializer.Deserialize<Snapshot>(File.ReadAllBytes(_path))
                    ?? throw new InvalidDataException("Missing processor credit journal");
            else
            {
                Initial = new Snapshot(Guid.NewGuid(), []);
                Save([]);
            }
        }
        catch
        {
            _lock.Dispose();
            throw;
        }
    }

    public void Save(Offer[] offers)
    {
        using (var replacement = new FileStream(_path + ".next", FileMode.Create, FileAccess.Write, FileShare.None))
        {
            JsonSerializer.Serialize(replacement, new Snapshot(Initial.ProcessorId, offers));
            replacement.Flush(flushToDisk: true);
        }
        File.Move(_path + ".next", _path, overwrite: true);
    }

    public void Dispose() => _lock.Dispose();
}
