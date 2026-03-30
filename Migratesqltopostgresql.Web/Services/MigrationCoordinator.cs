using System.Collections.Concurrent;
using Migratesqltopostgresql.Web.Models;

namespace Migratesqltopostgresql.Web.Services;

public sealed class MigrationCoordinator
{
    private readonly ConcurrentDictionary<string, MigrationJob> _jobs = new();
    private readonly SqlServerToPostgresMigrator _migrator;
    private readonly ILogger<MigrationCoordinator> _logger;

    public MigrationCoordinator(SqlServerToPostgresMigrator migrator, ILogger<MigrationCoordinator> logger)
    {
        _migrator = migrator;
        _logger = logger;
    }

    public string Start(
        string sourceDbName,
        string targetDbName,
        string sqlServerConnectionTemplate,
        string postgresAdminConnection)
    {
        var id = Guid.NewGuid().ToString("N");
        var job = new MigrationJob
        {
            Id = id,
            CreatedAtUtc = DateTimeOffset.UtcNow
        };
        _jobs[id] = job;

        _ = Task.Run(async () =>
        {
            try
            {
                await _migrator.RunAsync(
                    sourceDbName,
                    targetDbName,
                    sqlServerConnectionTemplate,
                    postgresAdminConnection,
                    (percent, message) =>
                {
                    job.Update(percent, message);
                    _logger.LogInformation("Migration {JobId}: {Progress}% {Message}", id, percent, message);
                });
                job.MarkSuccess();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Migration {JobId} failed", id);
                job.MarkFailure(ex.Message);
            }
        });

        return id;
    }

    public MigrationJob? Get(string id)
    {
        _jobs.TryGetValue(id, out var job);
        return job;
    }
}
