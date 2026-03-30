using Microsoft.AspNetCore.Mvc;
using Migratesqltopostgresql.Web.Models;
using Migratesqltopostgresql.Web.Services;

namespace Migratesqltopostgresql.Web.Controllers;

[ApiController]
[Route("api/migration")]
public sealed class MigrationController : ControllerBase
{
    private readonly MigrationCoordinator _coordinator;

    public MigrationController(MigrationCoordinator coordinator)
    {
        _coordinator = coordinator;
    }

    [HttpPost("start")]
    public IActionResult Start([FromBody] MigrationStartRequest request)
    {
        var source = request.DbName?.Trim() ?? string.Empty;
        var target = string.IsNullOrWhiteSpace(request.TargetDbName) ? source : request.TargetDbName.Trim();

        if (string.IsNullOrWhiteSpace(source))
        {
            return BadRequest(new { message = "Source DB name is required." });
        }

        if (string.IsNullOrWhiteSpace(target))
        {
            return BadRequest(new { message = "Target DB name is required." });
        }

        var sqlServerConnectionTemplate = request.SqlServerConnectionTemplate?.Trim();
        var postgresAdminConnection = request.PostgresAdminConnection?.Trim();

        if (string.IsNullOrWhiteSpace(sqlServerConnectionTemplate))
        {
            return BadRequest(new { message = "SQL Server connection template is required." });
        }

        if (string.IsNullOrWhiteSpace(postgresAdminConnection))
        {
            return BadRequest(new { message = "PostgreSQL admin connection string is required." });
        }

        var jobId = _coordinator.Start(source, target, sqlServerConnectionTemplate, postgresAdminConnection);
        return Ok(new { jobId });
    }

    [HttpGet("status/{jobId}")]
    public IActionResult Status(string jobId)
    {
        var job = _coordinator.Get(jobId);
        if (job is null)
        {
            return NotFound(new { message = "Job not found" });
        }

        return Ok(new
        {
            job.Id,
            createdAt = job.CreatedAtUtc,
            job.Status,
            job.Progress,
            job.Done,
            job.Success,
            job.Error,
            logs = job.Logs
        });
    }
}
