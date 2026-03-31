namespace Migratesqltopostgresql.Web.Models;

public sealed class RenameToSnakeCaseRequest
{
    public string TargetDbName { get; set; } = string.Empty;
    public string PostgresAdminConnection { get; set; } = string.Empty;
}

public sealed class RenameJob
{
    public string Id { get; init; } = string.Empty;
    public DateTimeOffset StartedAt { get; init; } = DateTimeOffset.UtcNow;
    public bool Done { get; set; }
    public bool Success { get; set; }
    public string? Error { get; set; }
    public RenameToSnakeCaseResult? Result { get; set; }
    private readonly List<string> _logs = new();
    private readonly object _lock = new();

    public void Log(string message)
    {
        lock (_lock) _logs.Add($"[{DateTimeOffset.UtcNow:HH:mm:ss.fff}] {message}");
    }

    public IReadOnlyList<string> GetLogs()
    {
        lock (_lock) return _logs.ToList();
    }
}

public sealed class RenameToSnakeCaseResult
{
    public bool Success { get; set; }
    public string? Error { get; set; }
    public int SchemasRenamed { get; set; }
    public int TablesRenamed { get; set; }
    public int ColumnsRenamed { get; set; }
    public int IndexesRenamed { get; set; }
    public int ConstraintsRenamed { get; set; }
    public int SequencesRenamed { get; set; }
    public int ViewsRenamed { get; set; }
    public int FunctionsRenamed { get; set; }
    public List<string> Skipped { get; set; } = new();
    public List<string> Changes { get; set; } = new();
}
