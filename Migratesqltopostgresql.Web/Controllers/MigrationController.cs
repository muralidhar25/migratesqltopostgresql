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
    private readonly RenameJobCoordinator _renameCoordinator;
    private readonly IConfiguration _configuration;

    public MigrationController(
        MigrationCoordinator coordinator,
        RenameJobCoordinator renameCoordinator,
        IConfiguration configuration)
    {
        _coordinator = coordinator;
        _renameCoordinator = renameCoordinator;
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
        var migrationMode = request.MigrationMode?.Trim()?.ToLowerInvariant() ?? "schemaanddata";

        if (string.IsNullOrWhiteSpace(sqlServerConnectionTemplate))
        {
            return BadRequest(new { message = "SQL Server connection template is required." });
        }

        if (string.IsNullOrWhiteSpace(postgresAdminConnection))
        {
            return BadRequest(new { message = "PostgreSQL admin connection string is required." });
        }

        var schemaOnly = migrationMode == "schemaonly";
        var jobId = _coordinator.Start(source, target, sqlServerConnectionTemplate, postgresAdminConnection, schemaOnly);
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

    [HttpPost("convert-definitions")]
    public async Task<IActionResult> ConvertDefinitions([FromBody] ConvertDefinitionsRequest request)
    {
        var targetDb = request.TargetDbName?.Trim();
        var postgresAdminConnection = request.PostgresAdminConnection?.Trim();

        if (string.IsNullOrWhiteSpace(targetDb))
        {
            return BadRequest(new { success = false, message = "Target database name is required." });
        }

        if (string.IsNullOrWhiteSpace(postgresAdminConnection))
        {
            return BadRequest(new { success = false, message = "PostgreSQL connection string is required." });
        }

        try
        {
            var migrator = new SqlServerToPostgresMigrator(_configuration);
            await migrator.UpdateConvertedDefinitionsAsync(postgresAdminConnection, targetDb);
            return Ok(new { success = true, message = "Converted definitions updated successfully." });
        }
        catch (Exception ex)
        {
            return Ok(new { success = false, message = ex.Message });
        }
    }

    [HttpPost("compare")]
    public async Task<IActionResult> CompareDatabases([FromBody] DatabaseComparisonRequest request)
    {
        try
        {
            var comparison = await CompareDatabasesAsync(
                request.SourceDbName,
                request.TargetDbName,
                request.SqlServerConnectionTemplate,
                request.PostgresAdminConnection
            );

            return Ok(comparison);
        }
        catch (Exception ex)
        {
            return Ok(new
            {
                success = false,
                message = ex.Message
            });
        }
    }

    private async Task<DatabaseComparisonResult> CompareDatabasesAsync(
        string sourceDb,
        string targetDb,
        string sqlTemplate,
        string pgAdmin)
    {
        var result = new DatabaseComparisonResult
        {
            SourceDatabase = sourceDb,
            TargetDatabase = targetDb
        };

        // Build connection strings
        var sqlCs = sqlTemplate.Contains("{dbname}", StringComparison.OrdinalIgnoreCase)
            ? sqlTemplate.Replace("{dbname}", sourceDb, StringComparison.OrdinalIgnoreCase)
            : sqlTemplate;

        var pgCs = new NpgsqlConnectionStringBuilder(pgAdmin) { Database = targetDb }.ConnectionString;

        // Get SQL Server tables and row counts
        var sqlServerTables = new Dictionary<string, (string schema, long rows)>();
        await using (var sqlConn = new SqlConnection(sqlCs))
        {
            await sqlConn.OpenAsync();

            const string sqlQuery = @"
                SELECT 
                    s.name AS SchemaName,
                    t.name AS TableName,
                    SUM(p.[rows]) AS RowCnt
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.partitions p ON t.object_id = p.object_id
                WHERE t.is_ms_shipped = 0 AND p.index_id IN (0,1)
                GROUP BY s.name, t.name
                ORDER BY s.name, t.name";

            await using var cmd = new SqlCommand(sqlQuery, sqlConn);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var schema = reader.GetString(0);
                var table = reader.GetString(1);
                var rows = reader.GetInt64(2);
                sqlServerTables[$"{schema}.{table}"] = (schema, rows);
            }
        }

        // Get PostgreSQL tables and row counts
        var postgresTables = new Dictionary<string, (string schema, long rows)>();
        await using (var pgConn = new NpgsqlConnection(pgCs))
        {
            await pgConn.OpenAsync();

            const string pgQuery = @"
                SELECT 
                    schemaname,
                    tablename
                FROM pg_tables
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY schemaname, tablename";

            var tableList = new List<(string schema, string table)>();
            await using (var cmd = new NpgsqlCommand(pgQuery, pgConn))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    tableList.Add((reader.GetString(0), reader.GetString(1)));
                }
            }

            // Get row counts for each table
            foreach (var (schema, table) in tableList)
            {
                try
                {
                    var countQuery = $"SELECT COUNT(*) FROM \"{schema}\".\"{table}\"";
                    await using var countCmd = new NpgsqlCommand(countQuery, pgConn);
                    var count = (long)(await countCmd.ExecuteScalarAsync() ?? 0L);
                    postgresTables[$"{schema}.{table}"] = (schema, count);
                }
                catch
                {
                    postgresTables[$"{schema}.{table}"] = (schema, -1L);
                }
            }
        }

        // Build comparison results
        result.SqlServerTableCount = sqlServerTables.Count;
        result.PostgresTableCount = postgresTables.Count;

        var allTableKeys = sqlServerTables.Keys.Union(postgresTables.Keys).Distinct().OrderBy(k => k);

        foreach (var key in allTableKeys)
        {
            var existsInSql = sqlServerTables.TryGetValue(key, out var sqlInfo);
            var existsInPg = postgresTables.TryGetValue(key, out var pgInfo);

            var parts = key.Split('.');
            var schema = parts[0];
            var tableName = parts[1];

            var tableComp = new TableComparison
            {
                Schema = schema,
                TableName = tableName,
                SqlServerRows = existsInSql ? sqlInfo.rows : 0,
                PostgresRows = existsInPg ? pgInfo.rows : 0,
                ExistsInSqlServer = existsInSql,
                ExistsInPostgres = existsInPg
            };

            result.Tables.Add(tableComp);

            if (existsInSql && !existsInPg)
            {
                result.MissingInPostgres.Add(key);
            }
            else if (!existsInSql && existsInPg)
            {
                result.ExtraInPostgres.Add(key);
            }
        }

        // Group by schema
        var schemaGroups = result.Tables.GroupBy(t => t.Schema);
        foreach (var group in schemaGroups)
        {
            result.Schemas.Add(new SchemaComparison
            {
                SchemaName = group.Key,
                SqlServerTables = group.Count(t => t.ExistsInSqlServer),
                PostgresTables = group.Count(t => t.ExistsInPostgres)
            });
        }

        return result;
    }

    [HttpPost("rename-to-snake-case")]
    public IActionResult RenameToSnakeCase([FromBody] RenameToSnakeCaseRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.TargetDbName))
            return BadRequest(new { success = false, message = "Target database name is required." });

        if (string.IsNullOrWhiteSpace(request.PostgresAdminConnection))
            return BadRequest(new { success = false, message = "PostgreSQL connection string is required." });

        var jobId = _renameCoordinator.Start(request.PostgresAdminConnection, request.TargetDbName);
        return Ok(new { jobId });
    }

    [HttpGet("rename-status/{jobId}")]
    public IActionResult RenameStatus(string jobId)
    {
        var job = _renameCoordinator.Get(jobId);
        if (job is null) return NotFound(new { message = "Job not found." });

        return Ok(new
        {
            done    = job.Done,
            success = job.Success,
            error   = job.Error,
            logs    = job.GetLogs(),
            result  = job.Done ? job.Result : null
        });
    }
}
