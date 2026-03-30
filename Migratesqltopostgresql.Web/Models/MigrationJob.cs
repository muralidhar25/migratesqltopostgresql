using System.Collections.Concurrent;

namespace Migratesqltopostgresql.Web.Models;

public sealed class MigrationJob
{
    private readonly ConcurrentQueue<string> _logs = new();

    public required string Id { get; init; }
    public required DateTimeOffset CreatedAtUtc { get; init; }
    public string Status { get; private set; } = "Queued";
    public int Progress { get; private set; }
    public bool Done { get; private set; }
    public bool Success { get; private set; }
    public string? Error { get; private set; }

    public IReadOnlyList<string> Logs => _logs.ToList();

    public void Update(int progress, string status)
    {
        Progress = Math.Clamp(progress, 0, 100);
        Status = status;
        _logs.Enqueue($"[{DateTimeOffset.UtcNow:O}] {status}");
        TrimLogs();
    }

    public void MarkSuccess()
    {
        Done = true;
        Success = true;
        Progress = 100;
        Status = "Done";
        _logs.Enqueue($"[{DateTimeOffset.UtcNow:O}] Migration completed successfully.");
        TrimLogs();
    }

    public void MarkFailure(string message)
    {
        Done = true;
        Success = false;
        Error = message;
        Status = "Failed";
        _logs.Enqueue($"[{DateTimeOffset.UtcNow:O}] ERROR: {message}");
        TrimLogs();
    }

    private void TrimLogs()
    {
        while (_logs.Count > 400 && _logs.TryDequeue(out _))
        {
        }
    }
}
