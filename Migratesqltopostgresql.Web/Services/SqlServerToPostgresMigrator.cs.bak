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

        var sqlTemplate = string.IsNullOrWhiteSpace(sqlServerConnectionTemplate)
            ? _configuration["Migration:SqlServerConnectionTemplate"]
            : sqlServerConnectionTemplate;
        var pgAdmin = string.IsNullOrWhiteSpace(postgresAdminConnection)
            ? _configuration["Migration:PostgresAdminConnection"]
            : postgresAdminConnection;
        var batchSize = _configuration.GetValue("Migration:BatchSize", 1000);

        if (string.IsNullOrWhiteSpace(sqlTemplate))
        {
            throw new InvalidOperationException("Migration:SqlServerConnectionTemplate is required.");
        }

        if (string.IsNullOrWhiteSpace(pgAdmin))
        {
            throw new InvalidOperationException("Migration:PostgresAdminConnection is required.");
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
            await MigrateTransactionalAsync(sqlConn, pgAdmin, targetDbName, batchSize, progress);
            progress(100, "Migration completed.");
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

    private static string BuildSqlServerConnectionString(string sqlTemplate, string sourceDb)
    {
        if (sqlTemplate.Contains("{dbname}", StringComparison.OrdinalIgnoreCase))
        {
            return sqlTemplate.Replace("{dbname}", sourceDb, StringComparison.OrdinalIgnoreCase);
        }

        var builder = new SqlConnectionStringBuilder(sqlTemplate)
        {
            InitialCatalog = sourceDb
        };

        return builder.ConnectionString;
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
        Action<int, string> progress)
    {
        var targetCsBuilder = new NpgsqlConnectionStringBuilder(adminConn) { Database = targetDb };
        await using var targetConn = new NpgsqlConnection(targetCsBuilder.ConnectionString);
        await targetConn.OpenAsync();
        await using var tx = await targetConn.BeginTransactionAsync(IsolationLevel.Serializable);

        try
        {
            await using (var lockCmd = new NpgsqlCommand("SELECT pg_advisory_xact_lock(9172301)", targetConn, tx))
            {
                await lockCmd.ExecuteNonQueryAsync();
            }

            await using (var extCmd = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pgcrypto", targetConn, tx))
            {
                await extCmd.ExecuteNonQueryAsync();
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

            foreach (var schema in allSchemas)
            {
                await using var schemaCmd = new NpgsqlCommand($"CREATE SCHEMA IF NOT EXISTS {QuoteIdent(schema)}", targetConn, tx);
                await schemaCmd.ExecuteNonQueryAsync();
            }

            var tableCount = Math.Max(tables.Count, 1);
            for (var i = 0; i < tables.Count; i++)
            {
                var table = tables[i];
                await CreateTargetTableAsync(sourceConn, targetConn, tx, table);
                var pct = 20 + (int)Math.Round(((i + 1d) / tableCount) * 25d);
                progress(pct, $"Schema migrated: {table.Schema}.{table.Name}");
            }

            for (var i = 0; i < tables.Count; i++)
            {
                var table = tables[i];
                await CopyTableDataAsync(sourceConn, targetConn, tx, table, batchSize);
                var pct = 50 + (int)Math.Round(((i + 1d) / tableCount) * 40d);
                progress(pct, $"Data migrated: {table.Schema}.{table.Name}");
            }

            await PersistSourceObjectsAsync(targetConn, tx, routines, views);
            progress(95, "Functions, procedures, and views captured for conversion.");

            await tx.CommitAsync();
        }
        catch
        {
            await tx.RollbackAsync();
            throw;
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

            var def = ConvertDefault(col.DefaultDefinition);
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
        int batchSize)
    {
        await using var readCmd = new SqlCommand($"SELECT * FROM [{table.Schema}].[{table.Name}]", sourceConn);
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
        while (await reader.ReadAsync())
        {
            var values = new object?[reader.FieldCount];
            reader.GetValues(values);
            rows.Add(values);

            if (rows.Count >= batchSize)
            {
                await FlushRowsAsync(targetConn, tx, insertSql, rows);
                rows.Clear();
            }
        }

        if (rows.Count > 0)
        {
            await FlushRowsAsync(targetConn, tx, insertSql, rows);
        }
    }

    private static async Task FlushRowsAsync(
        NpgsqlConnection targetConn,
        NpgsqlTransaction tx,
        string insertSql,
        IReadOnlyList<object?[]> rows)
    {
        foreach (var row in rows)
        {
            await using var cmd = new NpgsqlCommand(insertSql, targetConn, tx);
            for (var i = 0; i < row.Length; i++)
            {
                cmd.Parameters.AddWithValue($"p{i}", row[i] ?? DBNull.Value);
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
        const string createSql = @"
            CREATE TABLE IF NOT EXISTS public.migration_source_objects (
                object_schema TEXT NOT NULL,
                object_name TEXT NOT NULL,
                object_type TEXT NOT NULL,
                source_definition TEXT,
                captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (object_schema, object_name, object_type)
            );";

        await using (var createCmd = new NpgsqlCommand(createSql, targetConn, tx))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        const string upsertSql = @"
            INSERT INTO public.migration_source_objects (object_schema, object_name, object_type, source_definition, captured_at)
            VALUES (@schema, @name, @type, @def, NOW())
            ON CONFLICT (object_schema, object_name, object_type)
            DO UPDATE SET source_definition = EXCLUDED.source_definition, captured_at = EXCLUDED.captured_at;";

        foreach (var routine in routines)
        {
            await using var cmd = new NpgsqlCommand(upsertSql, targetConn, tx);
            cmd.Parameters.AddWithValue("schema", routine.Schema);
            cmd.Parameters.AddWithValue("name", routine.Name);
            cmd.Parameters.AddWithValue("type", routine.Type);
            cmd.Parameters.AddWithValue("def", (object?)routine.Definition ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }

        foreach (var view in views)
        {
            await using var cmd = new NpgsqlCommand(upsertSql, targetConn, tx);
            cmd.Parameters.AddWithValue("schema", view.Schema);
            cmd.Parameters.AddWithValue("name", view.Name);
            cmd.Parameters.AddWithValue("type", "VIEW");
            cmd.Parameters.AddWithValue("def", (object?)view.Definition ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static string ConvertDefault(string? sqlDefault)
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

        if (result.Equals("getdate()", StringComparison.OrdinalIgnoreCase))
        {
            return "CURRENT_TIMESTAMP";
        }

        if (result.Equals("newid()", StringComparison.OrdinalIgnoreCase))
        {
            return "gen_random_uuid()";
        }

        if (result.StartsWith("N'", StringComparison.OrdinalIgnoreCase))
        {
            return $"'{result[2..]}";
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
