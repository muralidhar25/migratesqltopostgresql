using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Npgsql;
using Migratesqltopostgresql.Web.Models;
using Migratesqltopostgresql.Web.Services;

namespace Migratesqltopostgresql.Web.Controllers;

[ApiController]
[Route("api/migration")]
public sealed class MigrationController : ControllerBase
{
    private readonly MigrationCoordinator _coordinator;
    private readonly IConfiguration _configuration;

    public MigrationController(MigrationCoordinator coordinator, IConfiguration configuration)
    {
        _coordinator = coordinator;
        _configuration = configuration;
    }

    [HttpPost("test-connection")]
    public async Task<IActionResult> TestConnection([FromBody] TestConnectionRequest request)
    {
        var type = request.Type?.Trim().ToLowerInvariant();
        var connectionString = request.ConnectionString?.Trim();

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return BadRequest(new { success = false, message = "Connection string is required." });
        }

        try
        {
            if (type == "sqlserver")
            {
                // Replace {dbname} with "master" for the connectivity probe
                var probeCs = connectionString.Replace("{dbname}", "master", StringComparison.OrdinalIgnoreCase);
                await using var conn = new SqlConnection(probeCs);
                await conn.OpenAsync();
                var serverVersion = conn.ServerVersion;
                return Ok(new { success = true, message = $"Connected to SQL Server {serverVersion}." });
            }
            else if (type == "postgres")
            {
                await using var conn = new NpgsqlConnection(connectionString);
                await conn.OpenAsync();
                var serverVersion = conn.ServerVersion;
                return Ok(new { success = true, message = $"Connected to PostgreSQL {serverVersion}." });
            }
            else
            {
                return BadRequest(new { success = false, message = "Unknown connection type. Use 'sqlserver' or 'postgres'." });
            }
        }
        catch (Exception ex)
        {
            return Ok(new { success = false, message = ex.Message });
        }
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
