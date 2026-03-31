using System.Data;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace Migratesqltopostgresql.Web.Services;

public sealed class SqlServerToPostgresMigrator
{
    private readonly IConfiguration _configuration;

    public SqlServerToPostgresMigrator(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public async Task RunAsync(
        string sourceDb,
        string targetDb,
        string? sqlServerConnectionTemplate,
        string? postgresAdminConnection,
        bool schemaOnly,
        Action<int, string> progress)
    {
        if (string.IsNullOrWhiteSpace(sourceDb))
        {
            throw new InvalidOperationException("Source DB name is required.");
        }

        if (string.IsNullOrWhiteSpace(targetDb))
        {
            throw new InvalidOperationException("Target DB name is required.");
        }

        var sqlTemplate = sqlServerConnectionTemplate?.Trim();
        var pgAdmin = postgresAdminConnection?.Trim();
        var batchSize = _configuration.GetValue("Migration:BatchSize", 1000);
        var maxRetries = _configuration.GetValue("Migration:MaxRetries", 10);

        if (string.IsNullOrWhiteSpace(sqlTemplate))
        {
            throw new InvalidOperationException("SQL Server connection template is required.");
        }

        if (string.IsNullOrWhiteSpace(pgAdmin))
        {
            throw new InvalidOperationException("PostgreSQL admin connection is required.");
        }

        var sourceConnectionString = BuildSqlServerConnectionString(sqlTemplate, sourceDb.Trim());
        var targetDbName = targetDb.Trim();

        progress(5, "Connecting to SQL Server...");
        await using var sqlConn = new SqlConnection(sourceConnectionString);
        await sqlConn.OpenAsync();

        progress(10, "Ensuring PostgreSQL database exists...");
        var createdTarget = await EnsureTargetDatabaseAsync(pgAdmin, targetDbName);

        try
        {
            await MigrateTransactionalAsync(sqlConn, pgAdmin, targetDbName, batchSize, maxRetries, schemaOnly, progress);
            progress(100, schemaOnly ? "Schema migration completed." : "Migration completed.");
        }
        catch
        {
            if (createdTarget)
            {
                await DropTargetDatabaseAsync(pgAdmin, targetDbName);
            }

            throw;
        }
    }

    public async Task UpdateConvertedDefinitionsAsync(string? postgresAdminConnection, string targetDb)
    {
        var pgAdmin = postgresAdminConnection?.Trim();

        if (string.IsNullOrWhiteSpace(pgAdmin))
        {
            throw new InvalidOperationException("PostgreSQL admin connection is required.");
        }

        var targetCsBuilder = new NpgsqlConnectionStringBuilder(pgAdmin) { Database = targetDb.Trim() };
        await using var conn = new NpgsqlConnection(targetCsBuilder.ConnectionString);
        await conn.OpenAsync();

        const string updateSql = @"
            UPDATE migration.migration_source_objects
            SET converted_definition = @converted
            WHERE object_schema = @schema 
              AND object_name = @name 
              AND object_type = @type";

        const string selectSql = @"
            SELECT object_schema, object_name, object_type, source_definition
            FROM migration.migration_source_objects
            WHERE source_definition IS NOT NULL";

        var objects = new List<(string Schema, string Name, string Type, string Definition)>();
        await using (var selectCmd = new NpgsqlCommand(selectSql, conn))
        await using (var reader = await selectCmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                objects.Add((
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3)
                ));
            }
        }

        foreach (var obj in objects)
        {
            var converted = ConvertTSqlToPostgres(obj.Definition);
            await using var updateCmd = new NpgsqlCommand(updateSql, conn);
            updateCmd.Parameters.AddWithValue("schema", obj.Schema);
            updateCmd.Parameters.AddWithValue("name", obj.Name);
            updateCmd.Parameters.AddWithValue("type", obj.Type);
            updateCmd.Parameters.AddWithValue("converted", converted);
            await updateCmd.ExecuteNonQueryAsync();
        }
    }

    private static string BuildSqlServerConnectionString(string sqlTemplate, string sourceDb)
    {
        if (sqlTemplate.Contains("{dbname}", StringComparison.OrdinalIgnoreCase))
        {
            var connStr = sqlTemplate.Replace("{dbname}", sourceDb, StringComparison.OrdinalIgnoreCase);

            // Ensure timeout is set
            var builder = new SqlConnectionStringBuilder(connStr);
            if (builder.ConnectTimeout < 300) // Less than 5 minutes
            {
                builder.ConnectTimeout = 300; // 5 minutes minimum
            }
            return builder.ConnectionString;
        }

        var sqlBuilder = new SqlConnectionStringBuilder(sqlTemplate)
        {
            InitialCatalog = sourceDb
        };

        if (sqlBuilder.ConnectTimeout < 300)
        {
            sqlBuilder.ConnectTimeout = 300; // 5 minutes minimum
        }

        return sqlBuilder.ConnectionString;
    }

    private async Task<bool> EnsureTargetDatabaseAsync(string adminConn, string targetDb)
    {
        await using var conn = new NpgsqlConnection(adminConn);
        await conn.OpenAsync();

        await using (var checkCmd = new NpgsqlCommand("SELECT 1 FROM pg_database WHERE datname = @name", conn))
        {
            checkCmd.Parameters.AddWithValue("name", targetDb);
            var exists = await checkCmd.ExecuteScalarAsync();
            if (exists is not null)
            {
                return false;
            }
        }

        await using var createCmd = new NpgsqlCommand($"CREATE DATABASE {QuoteIdent(targetDb)}", conn);
        await createCmd.ExecuteNonQueryAsync();
        return true;
    }

    private async Task DropTargetDatabaseAsync(string adminConn, string targetDb)
    {
        await using var conn = new NpgsqlConnection(adminConn);
        await conn.OpenAsync();

        await using (var terminate = new NpgsqlCommand(@"
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = @name AND pid <> pg_backend_pid();
        ", conn))
        {
            terminate.Parameters.AddWithValue("name", targetDb);
            await terminate.ExecuteNonQueryAsync();
        }

        await using var dropCmd = new NpgsqlCommand($"DROP DATABASE IF EXISTS {QuoteIdent(targetDb)}", conn);
        await dropCmd.ExecuteNonQueryAsync();
    }

    private async Task MigrateTransactionalAsync(
        SqlConnection sourceConn,
        string adminConn,
        string targetDb,
        int batchSize,
        int maxRetries,
        bool schemaOnly,
        Action<int, string> progress)
    {
        var targetCsBuilder = new NpgsqlConnectionStringBuilder(adminConn) 
        { 
            Database = targetDb,
            CommandTimeout = 0,     // 0 = infinite timeout for commands
            Timeout = 1024,         // Max allowed: ~17 minutes connection timeout
            KeepAlive = 30,         // Keep connection alive every 30 seconds
            MaxPoolSize = 1         // Single connection for migration
        };

        await using var targetConn = new NpgsqlConnection(targetCsBuilder.ConnectionString);
        await targetConn.OpenAsync();

        try
        {
            // Initialize database extensions and schemas (one-time setup)
            await using (var tx = await targetConn.BeginTransactionAsync(IsolationLevel.ReadCommitted))
            {
                await using (var lockCmd = new NpgsqlCommand("SELECT pg_advisory_xact_lock(9172301)", targetConn, tx))
                {
                    await lockCmd.ExecuteNonQueryAsync();
                }

                await using (var extCmd = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pgcrypto", targetConn, tx))
                {
                    await extCmd.ExecuteNonQueryAsync();
                }

                await tx.CommitAsync();
            }

            var tables = await GetSourceTablesAsync(sourceConn);
            var routines = await GetSourceRoutinesAsync(sourceConn);
            var views = await GetSourceViewsAsync(sourceConn);

            var allSchemas = tables.Select(t => t.Schema)
                .Concat(routines.Select(r => r.Schema))
                .Concat(views.Select(v => v.Schema))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x)
                .ToList();

            // Create all schemas in one transaction
            await using (var tx = await targetConn.BeginTransactionAsync(IsolationLevel.ReadCommitted))
            {
                foreach (var schema in allSchemas)
                {
                    await using var schemaCmd = new NpgsqlCommand($"CREATE SCHEMA IF NOT EXISTS {QuoteIdent(schema)}", targetConn, tx);
                    await schemaCmd.ExecuteNonQueryAsync();
                }
                await tx.CommitAsync();
            }

            var tableCount = Math.Max(tables.Count, 1);

            // Create all table schemas in separate transactions per table
            for (var i = 0; i < tables.Count; i++)
            {
                var table = tables[i];

                // Check if table already exists
                var tableExists = await TableExistsAsync(targetConn, table);
                if (tableExists)
                {
                    progress(20 + (int)Math.Round(((i + 1d) / tableCount) * 25d), 
                             $"Schema exists, skipping: {table.Schema}.{table.Name}");
                    continue;
                }

                try
                {
                    await ExecuteWithRetryAsync(async () =>
                    {
                        await using var tx = await targetConn.BeginTransactionAsync(IsolationLevel.ReadCommitted);
                        await CreateTargetTableAsync(sourceConn, targetConn, tx, table);
                        await tx.CommitAsync();
                    }, $"Creating schema for {table.Schema}.{table.Name}", progress, maxRetries);

                    var pct = 20 + (int)Math.Round(((i + 1d) / tableCount) * 25d);
                    progress(pct, $"Schema migrated: {table.Schema}.{table.Name}");
                }
                catch (Exception ex) when (IsPermissionError(ex))
                {
                    progress(0, $"Permission denied for {table.Schema}.{table.Name}, skipping...");
                    continue;
                }
            }

            // Copy data for each table in separate transactions (skip if schema-only mode)
            if (!schemaOnly)
            {
                for (var i = 0; i < tables.Count; i++)
                {
                    var table = tables[i];
                    var basePct = 50 + (int)Math.Round((i / (double)tableCount) * 40d);

                    // Check if table has data already
                    var hasData = await TableHasDataAsync(targetConn, table);
                    if (hasData)
                    {
                        progress(50 + (int)Math.Round(((i + 1d) / tableCount) * 40d),
                                 $"Data exists, skipping: {table.Schema}.{table.Name}");
                        continue;
                    }

                    try
                    {
                        await ExecuteWithRetryAsync(async () =>
                        {
                            await using var tx = await targetConn.BeginTransactionAsync(IsolationLevel.ReadCommitted);
                            await CopyTableDataAsync(sourceConn, targetConn, tx, table, batchSize, (rowCount) =>
                            {
                                progress(basePct, $"Copying data: {table.Schema}.{table.Name} ({rowCount} rows)");
                            });
                            await tx.CommitAsync();
                        }, $"Copying data for {table.Schema}.{table.Name}", progress, maxRetries);

                        var pct = 50 + (int)Math.Round(((i + 1d) / tableCount) * 40d);
                        progress(pct, $"Data migrated: {table.Schema}.{table.Name}");
                    }
                    catch (Exception ex) when (IsPermissionError(ex))
                    {
                        progress(0, $"Permission denied for {table.Schema}.{table.Name}, skipping...");
                        continue;
                    }
                }
            }
            else
            {
                progress(90, "Skipping data copy (schema-only mode)");
            }

            // Persist source objects (procedures, views, functions)
            await using (var tx = await targetConn.BeginTransactionAsync(IsolationLevel.ReadCommitted))
            {
                await PersistSourceObjectsAsync(targetConn, tx, routines, views);
                await tx.CommitAsync();
            }
            progress(95, "Functions, procedures, and views captured for conversion.");
            progress(100, "Migration completed successfully.");
        }
        catch (Exception ex)
        {
            progress(0, $"Migration failed: {ex.Message}");
            throw;
        }
    }

    private static async Task ExecuteWithRetryAsync(
        Func<Task> action,
        string operationName,
        Action<int, string> progress,
        int maxRetries = 10)
    {
        var retryCount = 0;
        while (true)
        {
            try
            {
                await action();
                return;
            }
            catch (NpgsqlException ex) when (retryCount < maxRetries && IsTransientError(ex))
            {
                retryCount++;
                // Exponential backoff with max cap of 60 seconds
                var exponentialDelay = Math.Pow(2, retryCount);
                var delay = TimeSpan.FromSeconds(Math.Min(exponentialDelay, 60));
                progress(0, $"Connection issue during {operationName}. Retrying in {delay.TotalSeconds}s... (Attempt {retryCount}/{maxRetries})");
                await Task.Delay(delay);
            }
            catch (TimeoutException) when (retryCount < maxRetries)
            {
                retryCount++;
                // Exponential backoff with max cap of 60 seconds
                var exponentialDelay = Math.Pow(2, retryCount);
                var delay = TimeSpan.FromSeconds(Math.Min(exponentialDelay, 60));
                progress(0, $"Timeout during {operationName}. Retrying in {delay.TotalSeconds}s... (Attempt {retryCount}/{maxRetries})");
                await Task.Delay(delay);
            }
        }
    }

    private static bool IsTransientError(NpgsqlException ex)
    {
        // Common transient error codes in PostgreSQL
        var transientErrors = new[]
        {
            "53300", // too_many_connections
            "53400", // configuration_limit_exceeded
            "08000", // connection_exception
            "08003", // connection_does_not_exist
            "08006", // connection_failure
            "08001", // sqlclient_unable_to_establish_sqlconnection
            "08004", // sqlserver_rejected_establishment_of_sqlconnection
            "57P03", // cannot_connect_now
            "58000", // system_error
            "58030", // io_error
        };

        return transientErrors.Contains(ex.SqlState) ||
               ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
               ex.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsPermissionError(Exception ex)
    {
        if (ex is NpgsqlException pgEx)
        {
            // PostgreSQL permission error codes
            return pgEx.SqlState == "42501" || // insufficient_privilege
                   pgEx.SqlState == "42P01" || // undefined_table  
                   pgEx.Message.Contains("permission denied", StringComparison.OrdinalIgnoreCase);
        }

        if (ex is SqlException sqlEx)
        {
            // SQL Server permission error codes
            return sqlEx.Number == 229 || // OBJECT permission denied
                   sqlEx.Number == 230 || // COLUMN permission denied
                   sqlEx.Number == 297 || // No permission on database
                   sqlEx.Message.Contains("permission", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private static async Task<bool> TableExistsAsync(NpgsqlConnection conn, SqlObjectRef table)
    {
        try
        {
            const string query = @"
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_schema = @schema 
                    AND table_name = @table
                )";

            await using var cmd = new NpgsqlCommand(query, conn);
            cmd.Parameters.AddWithValue("schema", table.Schema);
            cmd.Parameters.AddWithValue("table", table.Name);
            return (bool)(await cmd.ExecuteScalarAsync() ?? false);
        }
        catch
        {
            return false;
        }
    }

    private static async Task<bool> TableHasDataAsync(NpgsqlConnection conn, SqlObjectRef table)
    {
        try
        {
            var query = $"SELECT EXISTS (SELECT 1 FROM \"{table.Schema}\".\"{table.Name}\" LIMIT 1)";
            await using var cmd = new NpgsqlCommand(query, conn);
            return (bool)(await cmd.ExecuteScalarAsync() ?? false);
        }
        catch
        {
            return false;
        }
    }

    private static async Task<List<SqlObjectRef>> GetSourceTablesAsync(SqlConnection sourceConn)
    {
        const string sql = @"
            SELECT s.name, t.name
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE t.is_ms_shipped = 0
            ORDER BY s.name, t.name";

        var list = new List<SqlObjectRef>();
        await using var cmd = new SqlCommand(sql, sourceConn);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(new SqlObjectRef(reader.GetString(0), reader.GetString(1)));
        }

        return list;
    }

    private static async Task<List<SourceRoutine>> GetSourceRoutinesAsync(SqlConnection sourceConn)
    {
        const string sql = @"
            SELECT s.name, o.name, o.type_desc, m.definition
            FROM sys.objects o
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            LEFT JOIN sys.sql_modules m ON m.object_id = o.object_id
            WHERE o.type IN ('FN','IF','TF','P') AND o.is_ms_shipped = 0
            ORDER BY s.name, o.name";

        var list = new List<SourceRoutine>();
        await using var cmd = new SqlCommand(sql, sourceConn);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(new SourceRoutine(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3)));
        }

        return list;
    }

    private static async Task<List<SourceView>> GetSourceViewsAsync(SqlConnection sourceConn)
    {
        const string sql = @"
            SELECT s.name, v.name, m.definition
            FROM sys.views v
            JOIN sys.schemas s ON s.schema_id = v.schema_id
            LEFT JOIN sys.sql_modules m ON m.object_id = v.object_id
            WHERE v.is_ms_shipped = 0
            ORDER BY s.name, v.name";

        var list = new List<SourceView>();
        await using var cmd = new SqlCommand(sql, sourceConn);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(new SourceView(
                reader.GetString(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2)));
        }

        return list;
    }

    private static async Task CreateTargetTableAsync(
        SqlConnection sourceConn,
        NpgsqlConnection targetConn,
        NpgsqlTransaction tx,
        SqlObjectRef table)
    {
        var columns = await GetTableColumnsAsync(sourceConn, table);
        var pk = await GetPrimaryKeyColumnsAsync(sourceConn, table);

        var colDefs = new List<string>();
        foreach (var col in columns)
        {
            var type = MapType(col.DataType, col.MaxLength, col.Precision, col.Scale);
            var parts = new List<string> { $"{QuoteIdent(col.Name)} {type}" };

            if (col.IsIdentity)
            {
                parts.Add("GENERATED BY DEFAULT AS IDENTITY");
            }

            var def = ConvertDefault(col.DefaultDefinition, col.DataType, type);
            if (!string.IsNullOrWhiteSpace(def))
            {
                parts.Add($"DEFAULT {def}");
            }

            if (!col.IsNullable)
            {
                parts.Add("NOT NULL");
            }

            colDefs.Add(string.Join(" ", parts));
        }

        if (pk.Count > 0)
        {
            colDefs.Add($"PRIMARY KEY ({string.Join(", ", pk.Select(QuoteIdent))})");
        }

        var ddl = $@"
            DROP TABLE IF EXISTS {QuoteIdent(table.Schema)}.{QuoteIdent(table.Name)} CASCADE;
            CREATE TABLE {QuoteIdent(table.Schema)}.{QuoteIdent(table.Name)} (
                {string.Join(",\n                ", colDefs)}
            );";

        await using var cmd = new NpgsqlCommand(ddl, targetConn, tx);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<List<TableColumn>> GetTableColumnsAsync(SqlConnection sourceConn, SqlObjectRef table)
    {
        const string sql = @"
            SELECT c.name, ty.name, c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, dc.definition
            FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
            WHERE c.object_id = OBJECT_ID(@obj)
            ORDER BY c.column_id";

        var list = new List<TableColumn>();
        await using var cmd = new SqlCommand(sql, sourceConn);
        cmd.Parameters.AddWithValue("@obj", $"{table.Schema}.{table.Name}");
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(new TableColumn(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetInt16(2),
                reader.GetByte(3),
                reader.GetByte(4),
                reader.GetBoolean(5),
                reader.GetBoolean(6),
                reader.IsDBNull(7) ? null : reader.GetString(7)));
        }

        return list;
    }

    private static async Task<List<string>> GetPrimaryKeyColumnsAsync(SqlConnection sourceConn, SqlObjectRef table)
    {
        const string sql = @"
            SELECT c.name
            FROM sys.key_constraints kc
            JOIN sys.tables t ON t.object_id = kc.parent_object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = kc.unique_index_id
            JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
            WHERE kc.type = 'PK' AND s.name = @schema AND t.name = @table
            ORDER BY ic.key_ordinal";

        var list = new List<string>();
        await using var cmd = new SqlCommand(sql, sourceConn);
        cmd.Parameters.AddWithValue("@schema", table.Schema);
        cmd.Parameters.AddWithValue("@table", table.Name);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(reader.GetString(0));
        }

        return list;
    }

    private static async Task CopyTableDataAsync(
        SqlConnection sourceConn,
        NpgsqlConnection targetConn,
        NpgsqlTransaction tx,
        SqlObjectRef table,
        int batchSize,
        Action<int> progressCallback)
    {
        await using var readCmd = new SqlCommand($"SELECT * FROM [{table.Schema}].[{table.Name}]", sourceConn);
        readCmd.CommandTimeout = 0; // 0 = infinite timeout for reading large tables
        await using var reader = await readCmd.ExecuteReaderAsync();

        var colNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
        if (colNames.Count == 0)
        {
            return;
        }

        var insertSql = $"INSERT INTO {QuoteIdent(table.Schema)}.{QuoteIdent(table.Name)} " +
                        $"({string.Join(", ", colNames.Select(QuoteIdent))}) VALUES " +
                        $"({string.Join(", ", colNames.Select((_, i) => $"@p{i}"))})";

        var rows = new List<object?[]>(batchSize);
        var totalRows = 0;
        while (await reader.ReadAsync())
        {
            var values = new object?[reader.FieldCount];
            reader.GetValues(values);
            rows.Add(values);

            if (rows.Count >= batchSize)
            {
                await FlushRowsAsync(targetConn, tx, insertSql, rows);
                totalRows += rows.Count;
                progressCallback(totalRows);
                rows.Clear();
            }
        }

        if (rows.Count > 0)
        {
            await FlushRowsAsync(targetConn, tx, insertSql, rows);
            totalRows += rows.Count;
            progressCallback(totalRows);
        }
    }

    private static async Task FlushRowsAsync(
        NpgsqlConnection targetConn,
        NpgsqlTransaction tx,
        string insertSql,
        IReadOnlyList<object?[]> rows)
    {
        if (rows.Count == 0)
        {
            return;
        }

        // Use prepared statement for better performance
        await using var cmd = new NpgsqlCommand(insertSql, targetConn, tx);

        // Add parameters for the first row to define the command structure
        for (var i = 0; i < rows[0].Length; i++)
        {
            cmd.Parameters.Add(new NpgsqlParameter($"p{i}", DBNull.Value));
        }

        await cmd.PrepareAsync();

        // Execute for each row
        foreach (var row in rows)
        {
            for (var i = 0; i < row.Length; i++)
            {
                cmd.Parameters[i].Value = row[i] ?? DBNull.Value;
            }

            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static async Task PersistSourceObjectsAsync(
        NpgsqlConnection targetConn,
        NpgsqlTransaction tx,
        IReadOnlyList<SourceRoutine> routines,
        IReadOnlyList<SourceView> views)
    {
        // Create migration schema if it doesn't exist
        const string createSchemaSql = "CREATE SCHEMA IF NOT EXISTS migration";
        await using (var schemaCmd = new NpgsqlCommand(createSchemaSql, targetConn, tx))
        {
            await schemaCmd.ExecuteNonQueryAsync();
        }

        const string createSql = @"
            CREATE TABLE IF NOT EXISTS migration.migration_source_objects (
                object_schema TEXT NOT NULL,
                object_name TEXT NOT NULL,
                object_type TEXT NOT NULL,
                source_definition TEXT,
                converted_definition TEXT,
                captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (object_schema, object_name, object_type)
            );";

        await using (var createCmd = new NpgsqlCommand(createSql, targetConn, tx))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        const string upsertSql = @"
            INSERT INTO migration.migration_source_objects (object_schema, object_name, object_type, source_definition, converted_definition, captured_at)
            VALUES (@schema, @name, @type, @def, @converted, NOW())
            ON CONFLICT (object_schema, object_name, object_type)
            DO UPDATE SET source_definition = EXCLUDED.source_definition, converted_definition = EXCLUDED.converted_definition, captured_at = EXCLUDED.captured_at;";

        foreach (var routine in routines)
        {
            var convertedDef = ConvertTSqlToPostgres(routine.Definition);
            await using var cmd = new NpgsqlCommand(upsertSql, targetConn, tx);
            cmd.Parameters.AddWithValue("schema", routine.Schema);
            cmd.Parameters.AddWithValue("name", routine.Name);
            cmd.Parameters.AddWithValue("type", routine.Type);
            cmd.Parameters.AddWithValue("def", (object?)routine.Definition ?? DBNull.Value);
            cmd.Parameters.AddWithValue("converted", (object?)convertedDef ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }

        foreach (var view in views)
        {
            var convertedDef = ConvertTSqlToPostgres(view.Definition);
            await using var cmd = new NpgsqlCommand(upsertSql, targetConn, tx);
            cmd.Parameters.AddWithValue("schema", view.Schema);
            cmd.Parameters.AddWithValue("name", view.Name);
            cmd.Parameters.AddWithValue("type", "VIEW");
            cmd.Parameters.AddWithValue("def", (object?)view.Definition ?? DBNull.Value);
            cmd.Parameters.AddWithValue("converted", (object?)convertedDef ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static string ConvertDefault(string? sqlDefault, string sourceDataType, string targetPgType)
    {
        if (string.IsNullOrWhiteSpace(sqlDefault))
        {
            return string.Empty;
        }

        var result = sqlDefault.Trim();
        while (result.StartsWith("(") && result.EndsWith(")") && result.Length > 1)
        {
            result = result[1..^1].Trim();
        }

        // Handle N-prefixed strings before function conversion
        if (result.StartsWith("N'", StringComparison.OrdinalIgnoreCase))
        {
            result = $"'{result[2..]}";
        }

        // Convert SQL Server system functions to PostgreSQL equivalents
        result = ConvertTSqlToPostgres(result);

        // Convert bit defaults (0/1) to boolean (false/true) for BOOLEAN columns
        if (targetPgType.Equals("BOOLEAN", StringComparison.OrdinalIgnoreCase) && 
            sourceDataType.Equals("bit", StringComparison.OrdinalIgnoreCase))
        {
            if (result == "0")
            {
                return "false";
            }
            if (result == "1")
            {
                return "true";
            }
        }

        return result;
    }

    private static string MapType(string type, short maxLen, byte precision, byte scale)
    {
        var normalized = type.ToLowerInvariant();
        return normalized switch
        {
            "bigint" => "BIGINT",
            "int" => "INTEGER",
            "smallint" => "SMALLINT",
            "tinyint" => "SMALLINT",
            "bit" => "BOOLEAN",
            "float" => "DOUBLE PRECISION",
            "real" => "REAL",
            "numeric" or "decimal" => $"NUMERIC({precision},{scale})",
            "money" or "smallmoney" => "NUMERIC(19,4)",
            "date" => "DATE",
            "datetime" or "datetime2" or "smalldatetime" or "datetimeoffset" => "TIMESTAMP",
            "time" => "TIME",
            "char" or "varchar" => maxLen <= 0 ? "TEXT" : $"VARCHAR({maxLen})",
            "nchar" or "nvarchar" => maxLen <= 0 ? "TEXT" : $"VARCHAR({Math.Max(maxLen / 2, 1)})",
            "text" or "ntext" or "xml" or "sql_variant" => "TEXT",
            "binary" or "varbinary" or "image" or "timestamp" or "rowversion" => "BYTEA",
            "uniqueidentifier" => "UUID",
            _ => "TEXT"
        };
    }

    private static string ConvertTSqlToPostgres(string? tsqlDefinition)
    {
        if (string.IsNullOrWhiteSpace(tsqlDefinition))
        {
            return string.Empty;
        }

        var result = tsqlDefinition;

        var replacements = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            // System functions
            { "SUSER_NAME()", "CURRENT_USER" },
            { "SUSER_SNAME()", "CURRENT_USER" },
            { "USER_NAME()", "CURRENT_USER" },
            { "SYSTEM_USER", "CURRENT_USER" },

            // Date/Time functions
            { "GETDATE()", "CURRENT_TIMESTAMP" },
            { "GETUTCDATE()", "CURRENT_TIMESTAMP" },
            { "SYSDATETIME()", "CURRENT_TIMESTAMP" },
            { "SYSUTCDATETIME()", "CURRENT_TIMESTAMP" },
            { "SYSDATETIMEOFFSET()", "CURRENT_TIMESTAMP" },
            { "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP" },

            // UUID/GUID functions
            { "NEWID()", "gen_random_uuid()" },
            { "NEWSEQUENTIALID()", "gen_random_uuid()" },

            // String functions
            { "LEN(", "LENGTH(" },
            { "CHARINDEX(", "POSITION(" },

            // Type conversions
            { "CONVERT(VARCHAR", "CAST(" },
            { "CONVERT(NVARCHAR", "CAST(" },
            { "CONVERT(INT", "CAST(" },
            { "CONVERT(DATETIME", "CAST(" },

            // Data types
            { "[NVARCHAR]", "VARCHAR" },
            { "[VARCHAR]", "VARCHAR" },
            { "[INT]", "INTEGER" },
            { "[DATETIME]", "TIMESTAMP" },
            { "[DATETIME2]", "TIMESTAMP" },
            { "[DATETIMEOFFSET]", "TIMESTAMP" },
            { "[BIT]", "BOOLEAN" },
            { "[UNIQUEIDENTIFIER]", "UUID" }
        };

        foreach (var (oldValue, newValue) in replacements)
        {
            result = System.Text.RegularExpressions.Regex.Replace(
                result, 
                System.Text.RegularExpressions.Regex.Escape(oldValue), 
                newValue, 
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        }

        // Replace ISNULL(x, y) with COALESCE(x, y)
        result = System.Text.RegularExpressions.Regex.Replace(
            result,
            @"\bISNULL\s*\(",
            "COALESCE(",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        // Remove square brackets from identifiers
        result = System.Text.RegularExpressions.Regex.Replace(result, @"\[([^\]]+)\]", "$1");

        return result;
    }

    private static string QuoteIdent(string value) => "\"" + value.Replace("\"", "\"\"") + "\"";

    private sealed record SqlObjectRef(string Schema, string Name);
    private sealed record SourceRoutine(string Schema, string Name, string Type, string? Definition);
    private sealed record SourceView(string Schema, string Name, string? Definition);

    private sealed record TableColumn(
        string Name,
        string DataType,
        short MaxLength,
        byte Precision,
        byte Scale,
        bool IsNullable,
        bool IsIdentity,
        string? DefaultDefinition);
}

