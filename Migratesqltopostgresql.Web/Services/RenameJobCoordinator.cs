using System.Collections.Concurrent;
using Migratesqltopostgresql.Web.Models;
using Migratesqltopostgresql.Web.Models;

namespace Migratesqltopostgresql.Web.Services;

public sealed class RenameJobCoordinator
{
    private readonly ConcurrentDictionary<string, RenameJob> _jobs = new();
    private readonly ILogger<RenameJobCoordinator> _logger;

    public RenameJobCoordinator(ILogger<RenameJobCoordinator> logger)
    {
        _logger = logger;
    }

    public string Start(string pgAdminConn, string targetDb)
    {
        var id = Guid.NewGuid().ToString("N");
        var job = new RenameJob { Id = id };
        _jobs[id] = job;

        _ = Task.Run(async () =>
        {
            try
            {
                var renamer = new SnakeCaseRenamer();
                var result = await renamer.RenameAsync(pgAdminConn, targetDb, job);
                job.Result = result;
                job.Success = result.Success;
                if (!result.Success) job.Error = result.Error;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Rename job {JobId} failed", id);
                job.Error = ex.Message;
                job.Success = false;
            }
            finally
            {
                job.Done = true;
            }
        });

        return id;
    }

    public RenameJob? Get(string id)
    {
        _jobs.TryGetValue(id, out var job);
        return job;
    }
}
