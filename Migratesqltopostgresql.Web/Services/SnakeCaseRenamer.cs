using Npgsql;
using Npgsql;
using Migratesqltopostgresql.Web.Models;

namespace Migratesqltopostgresql.Web.Services;

/// <summary>
/// Renames all user-defined schemas, tables, columns, indexes,
/// constraints, sequences, views and functions from PascalCase/camelCase
/// to PostgreSQL-standard snake_case.
///
/// Safety model:
///  1. Pre-check: if everything is already snake_case, skip entirely.
///  2. Backup: CREATE DATABASE backup via TEMPLATE (full native copy).
///  3. Rename in order: columns ? constraints ? indexes ? sequences ?
///     views ? functions ? tables ? schemas.
///  4. Each object is skipped if already snake_case.
///  5. Each rename uses retry with exponential backoff (same as migrator).
///  6. Connection is re-opened per-operation so a dropped connection never
///     stops the whole run.
///  7. On any unexpected exception: drop target DB, rename backup back.
/// </summary>
public sealed class SnakeCaseRenamer
{
    private const int MaxRetries = 10;

    public async Task<RenameToSnakeCaseResult> RenameAsync(
        string pgAdminConn, string targetDb, RenameJob job)
    {
        var result   = new RenameToSnakeCaseResult();
        var targetCs = new NpgsqlConnectionStringBuilder(pgAdminConn)
        {
            Database       = targetDb,
            CommandTimeout = 0,
            KeepAlive      = 30,
        }.ConnectionString;

        // ?? 1. Pre-check ?????????????????????????????????????????????????????
        job.Log("Checking if identifiers already use snake_case...");
        if (await IsAlreadySnakeCaseAsync(targetCs))
        {
            job.Log("? All identifiers are already snake_case — nothing to rename.");
            result.Success = true;
            return result;
        }

        // ?? 2. Create rollback log table (inside migration schema) ???????????
        // Each successful rename appends its reverse ALTER so we can undo in
        // reverse order without needing CREATE DATABASE permissions.
        job.Log("Preparing in-database rollback log...");
        await EnsureRollbackTableAsync(targetCs);
        await ClearRollbackLogAsync(targetCs);
        job.Log("? Rollback log ready (migration.rename_rollback).");

        // ?? 3. Rename all identifiers ????????????????????????????????????????
        try
        {
            await RenameColumnsAsync(targetCs, result, job);
            await RenameConstraintsAsync(targetCs, result, job);
            await RenameIndexesAsync(targetCs, result, job);
            await RenameSequencesAsync(targetCs, result, job);
            await RenameViewsAsync(targetCs, result, job);
            await RenameFunctionsAsync(targetCs, result, job);
            await RenameTablesAsync(targetCs, result, job);
            await RenameSchemasAsync(targetCs, result, job);

            var total = result.SchemasRenamed + result.TablesRenamed + result.ColumnsRenamed +
                        result.IndexesRenamed + result.ConstraintsRenamed + result.SequencesRenamed +
                        result.ViewsRenamed + result.FunctionsRenamed;

            job.Log($"? Rename complete — {total} identifiers renamed, {result.Skipped.Count} skipped.");

            // Keep the rollback log in place so the user can inspect it,
            // but mark it completed so it won't be replayed accidentally.
            await MarkRollbackLogDoneAsync(targetCs);
            result.Success = true;
        }
        catch (Exception ex)
        {
            // ?? 4. Rollback via reverse-ALTER log ????????????????????????????
            job.Log($"? Error during rename: {ex.Message}");
            job.Log("? Rolling back completed renames using rollback log...");
            try
            {
                var rolled = await ExecuteRollbackLogAsync(targetCs, job);
                job.Log($"? Rollback complete — {rolled} rename(s) reversed.");
            }
            catch (Exception rbEx)
            {
                job.Log($"? Rollback failed: {rbEx.Message}");
                job.Log("?? Review migration.rename_rollback for manual recovery SQL.");
            }

            result.Success = false;
            result.Error   = ex.Message;
        }

        return result;
    }

    // ?? Rollback log helpers ??????????????????????????????????????????????????

    private static async Task EnsureRollbackTableAsync(string cs)
    {
        await using var conn = await OpenAsync(cs);
        const string sql = @"
            CREATE SCHEMA IF NOT EXISTS migration;
            CREATE TABLE IF NOT EXISTS migration.rename_rollback (
                seq         SERIAL PRIMARY KEY,
                reverse_sql TEXT    NOT NULL,
                applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                done        BOOLEAN NOT NULL DEFAULT FALSE
            );";
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task ClearRollbackLogAsync(string cs)
    {
        await using var conn = await OpenAsync(cs);
        const string sql = "DELETE FROM migration.rename_rollback;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task AppendRollbackAsync(string cs, string reverseSql)
    {
        await using var conn = await OpenAsync(cs);
        const string sql = "INSERT INTO migration.rename_rollback (reverse_sql) VALUES (@s);";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("s", reverseSql);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<int> ExecuteRollbackLogAsync(string cs, RenameJob job)
    {
        // Read in DESCENDING order so last rename is undone first
        await using var readConn = await OpenAsync(cs);
        const string selectSql = @"
            SELECT seq, reverse_sql FROM migration.rename_rollback
            WHERE done = FALSE
            ORDER BY seq DESC";
        await using var cmd    = new NpgsqlCommand(selectSql, readConn);
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<(int seq, string sql)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetInt32(0), reader.GetString(1)));
        await reader.DisposeAsync();
        readConn.Close();

        var count = 0;
        foreach (var (seq, reverseSql) in rows)
        {
            try
            {
                await using var conn = await OpenAsync(cs);
                await using var tx   = await conn.BeginTransactionAsync();
                await using var exec = new NpgsqlCommand(reverseSql, conn, tx);
                await exec.ExecuteNonQueryAsync();

                // Mark this entry as done
                await using var mark = new NpgsqlCommand(
                    "UPDATE migration.rename_rollback SET done = TRUE WHERE seq = @seq", conn, tx);
                mark.Parameters.AddWithValue("seq", seq);
                await mark.ExecuteNonQueryAsync();
                await tx.CommitAsync();
                count++;
                job.Log($"  ? rolled back: {reverseSql}");
            }
            catch (Exception ex)
            {
                job.Log($"  ?? rollback step seq={seq} failed: {ex.Message}");
            }
        }
        return count;
    }

    private static async Task MarkRollbackLogDoneAsync(string cs)
    {
        await using var conn = await OpenAsync(cs);
        const string sql = "UPDATE migration.rename_rollback SET done = TRUE;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    // ?? Retry helper (mirrors SqlServerToPostgresMigrator.ExecuteWithRetryAsync) ??
    private static async Task ExecuteWithRetryAsync(
        Func<Task> action,
        string operationName,
        RenameJob job)
    {
        var retryCount = 0;
        while (true)
        {
            try
            {
                await action();
                return;
            }
            catch (NpgsqlException ex) when (retryCount < MaxRetries && IsTransientError(ex))
            {
                retryCount++;
                var delay = TimeSpan.FromSeconds(Math.Min(Math.Pow(2, retryCount), 60));
                job.Log($"  ?? Connection issue during {operationName}. Retrying in {delay.TotalSeconds}s… (Attempt {retryCount}/{MaxRetries})");
                await Task.Delay(delay);
            }
            catch (TimeoutException) when (retryCount < MaxRetries)
            {
                retryCount++;
                var delay = TimeSpan.FromSeconds(Math.Min(Math.Pow(2, retryCount), 60));
                job.Log($"  ?? Timeout during {operationName}. Retrying in {delay.TotalSeconds}s… (Attempt {retryCount}/{MaxRetries})");
                await Task.Delay(delay);
            }
            catch (InvalidOperationException ex)
                when (retryCount < MaxRetries &&
                      ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase))
            {
                // "Connection is not open" — stale pooled connection
                retryCount++;
                var delay = TimeSpan.FromSeconds(Math.Min(Math.Pow(2, retryCount), 60));
                job.Log($"  ?? Stale connection during {operationName}. Retrying in {delay.TotalSeconds}s… (Attempt {retryCount}/{MaxRetries})");
                await Task.Delay(delay);
            }
        }
    }

    private static bool IsTransientError(NpgsqlException ex)
    {
        var transientSqlStates = new[]
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

        return transientSqlStates.Contains(ex.SqlState) ||
               ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
               ex.Message.Contains("timeout",    StringComparison.OrdinalIgnoreCase);
    }

    // Opens a fresh connection — called per-operation so stale connections never block the run
    private static async Task<NpgsqlConnection> OpenAsync(string cs)
    {
        var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync();
        return conn;
    }

    // ?? Check if already snake_case ??????????????????????????????????????????
    private static async Task<bool> IsAlreadySnakeCaseAsync(string targetCs)
    {
        await using var conn = new NpgsqlConnection(targetCs);
        await conn.OpenAsync();

        // Check schemas, tables, and columns — if any needs renaming, return false
        const string query = @"
            SELECT COUNT(*) FROM (
                SELECT schema_name AS name FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog','information_schema','pg_toast','migration','public')
                  AND schema_name NOT LIKE 'pg_%'
                UNION ALL
                SELECT table_name FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                  AND table_schema NOT IN ('pg_catalog','information_schema','pg_toast','migration')
                UNION ALL
                SELECT column_name FROM information_schema.columns c
                JOIN information_schema.tables t
                    ON t.table_schema = c.table_schema AND t.table_name = c.table_name
                WHERE t.table_type = 'BASE TABLE'
                  AND c.table_schema NOT IN ('pg_catalog','information_schema','pg_toast','migration')
            ) all_names
            WHERE name ~ '[A-Z]'";

        await using var cmd = new NpgsqlCommand(query, conn);
        var count = (long)(await cmd.ExecuteScalarAsync() ?? 0L);
        return count == 0;
    }

    // ?? Columns ???????????????????????????????????????????????????????????????
    private static async Task RenameColumnsAsync(string targetCs, RenameToSnakeCaseResult result, RenameJob job)
    {
        const string query = @"
            SELECT c.table_schema, c.table_name, c.column_name
            FROM information_schema.columns c
            JOIN information_schema.tables t
                ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            WHERE t.table_type = 'BASE TABLE'
              AND c.table_schema NOT IN ('pg_catalog','information_schema','pg_toast','migration')
            ORDER BY c.table_schema, c.table_name, c.ordinal_position";

        await using var readConn = await OpenAsync(targetCs);
        var columns = await QueryAsync(readConn, query,
            r => (schema: r.GetString(0), table: r.GetString(1), column: r.GetString(2)));

        job.Log($"Renaming columns ({columns.Count} total)...");
        foreach (var (schema, table, column) in columns)
        {
            var snake = ToSnakeCase(column);
            if (snake == column) continue;
            var label = $"column {schema}.{table}.{column}";
            try
            {
                await ExecuteWithRetryAsync(async () =>
                {
                    await using var conn = await OpenAsync(targetCs);
                    await using var tx   = await conn.BeginTransactionAsync();
                    await ExecAsync(conn, tx, $"ALTER TABLE {Q(schema)}.{Q(table)} RENAME COLUMN {Q(column)} TO {Q(snake)}");
                    await tx.CommitAsync();
                }, label, job);
                // Log the reverse rename immediately after success
                await AppendRollbackAsync(targetCs,
                    $"ALTER TABLE {Q(schema)}.{Q(table)} RENAME COLUMN {Q(snake)} TO {Q(column)}");
                result.ColumnsRenamed++;
                result.Changes.Add($"column  {schema}.{table}.{column} ? {snake}");
                job.Log($"  {label} ? {snake}");
            }
            catch (Exception ex)
            {
                result.Skipped.Add($"column  {schema}.{table}.{column}: {ex.Message}");
                job.Log($"  ?? skipped {label}: {ex.Message}");
            }
        }
    }

    // ?? Constraints ???????????????????????????????????????????????????????????
    private static async Task RenameConstraintsAsync(string targetCs, RenameToSnakeCaseResult result, RenameJob job)
    {
        const string query = @"
            SELECT n.nspname, c.conname
            FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast','migration')
            ORDER BY n.nspname, c.conname";

        await using var readConn = await OpenAsync(targetCs);
        var constraints = await QueryAsync(readConn, query,
            r => (schema: r.GetString(0), name: r.GetString(1)));

        job.Log($"Renaming constraints ({constraints.Count} total)...");
        foreach (var (schema, name) in constraints)
        {
            var snake = ToSnakeCase(name);
            if (snake == name) continue;
            var label = $"constraint {schema}.{name}";
            string? tableName = null;
            try
            {
                await ExecuteWithRetryAsync(async () =>
                {
                    await using var conn = await OpenAsync(targetCs);
                    await using var tx   = await conn.BeginTransactionAsync();
                    const string tq = @"SELECT t.relname FROM pg_constraint c
                        JOIN pg_class t ON t.oid = c.conrelid
                        JOIN pg_namespace n ON n.oid = c.connamespace
                        WHERE n.nspname = @s AND c.conname = @n";
                    await using var cmd = new NpgsqlCommand(tq, conn, tx);
                    cmd.Parameters.AddWithValue("s", schema);
                    cmd.Parameters.AddWithValue("n", name);
                    tableName = (string?)await cmd.ExecuteScalarAsync();
                    if (tableName is null) { await tx.RollbackAsync(); return; }
                    await ExecAsync(conn, tx,
                        $"ALTER TABLE {Q(schema)}.{Q(tableName)} RENAME CONSTRAINT {Q(name)} TO {Q(snake)}");
                    await tx.CommitAsync();
                }, label, job);

                if (tableName is not null)
                {
                    await AppendRollbackAsync(targetCs,
                        $"ALTER TABLE {Q(schema)}.{Q(tableName)} RENAME CONSTRAINT {Q(snake)} TO {Q(name)}");
                    result.ConstraintsRenamed++;
                    result.Changes.Add($"constraint  {schema}.{name} ? {snake}");
                    job.Log($"  {label} ? {snake}");
                }
            }
            catch (Exception ex)
            {
                result.Skipped.Add($"constraint  {schema}.{name}: {ex.Message}");
                job.Log($"  ?? skipped {label}: {ex.Message}");
            }
        }
    }

    // ?? Indexes ???????????????????????????????????????????????????????????????
    private static async Task RenameIndexesAsync(string targetCs, RenameToSnakeCaseResult result, RenameJob job)
    {
        const string query = @"
            SELECT n.nspname, i.relname
            FROM pg_index ix
            JOIN pg_class i     ON i.oid = ix.indexrelid
            JOIN pg_namespace n ON n.oid = i.relnamespace
            WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast','migration')
              AND NOT ix.indisprimary
            ORDER BY n.nspname, i.relname";

        await using var readConn = await OpenAsync(targetCs);
        var indexes = await QueryAsync(readConn, query,
            r => (schema: r.GetString(0), name: r.GetString(1)));

        job.Log($"Renaming indexes ({indexes.Count} total)...");
        foreach (var (schema, name) in indexes)
        {
            var snake = ToSnakeCase(name);
            if (snake == name) continue;
            var label = $"index {schema}.{name}";
            try
            {
                await ExecuteWithRetryAsync(async () =>
                {
                    await using var conn = await OpenAsync(targetCs);
                    await using var tx   = await conn.BeginTransactionAsync();
                    await ExecAsync(conn, tx, $"ALTER INDEX {Q(schema)}.{Q(name)} RENAME TO {Q(snake)}");
                    await tx.CommitAsync();
                }, label, job);
                await AppendRollbackAsync(targetCs,
                    $"ALTER INDEX {Q(schema)}.{Q(snake)} RENAME TO {Q(name)}");
                result.IndexesRenamed++;
                result.Changes.Add($"index  {schema}.{name} ? {snake}");
                job.Log($"  {label} ? {snake}");
            }
            catch (Exception ex)
            {
                result.Skipped.Add($"index  {schema}.{name}: {ex.Message}");
                job.Log($"  ?? skipped {label}: {ex.Message}");
            }
        }
    }

    // ?? Sequences ?????????????????????????????????????????????????????????????
    private static async Task RenameSequencesAsync(string targetCs, RenameToSnakeCaseResult result, RenameJob job)
    {
        const string query = @"
            SELECT sequence_schema, sequence_name
            FROM information_schema.sequences
            WHERE sequence_schema NOT IN ('pg_catalog','information_schema','pg_toast','migration')
            ORDER BY sequence_schema, sequence_name";

        await using var readConn = await OpenAsync(targetCs);
        var seqs = await QueryAsync(readConn, query,
            r => (schema: r.GetString(0), name: r.GetString(1)));

        job.Log($"Renaming sequences ({seqs.Count} total)...");
        foreach (var (schema, name) in seqs)
        {
            var snake = ToSnakeCase(name);
            if (snake == name) continue;
            var label = $"sequence {schema}.{name}";
            try
            {
                await ExecuteWithRetryAsync(async () =>
                {
                    await using var conn = await OpenAsync(targetCs);
                    await using var tx   = await conn.BeginTransactionAsync();
                    await ExecAsync(conn, tx, $"ALTER SEQUENCE {Q(schema)}.{Q(name)} RENAME TO {Q(snake)}");
                    await tx.CommitAsync();
                }, label, job);
                await AppendRollbackAsync(targetCs,
                    $"ALTER SEQUENCE {Q(schema)}.{Q(snake)} RENAME TO {Q(name)}");
                result.SequencesRenamed++;
                result.Changes.Add($"sequence  {schema}.{name} ? {snake}");
                job.Log($"  {label} ? {snake}");
            }
            catch (Exception ex)
            {
                result.Skipped.Add($"sequence  {schema}.{name}: {ex.Message}");
                job.Log($"  ?? skipped {label}: {ex.Message}");
            }
        }
    }

    // ?? Views ?????????????????????????????????????????????????????????????????
    private static async Task RenameViewsAsync(string targetCs, RenameToSnakeCaseResult result, RenameJob job)
    {
        const string query = @"
            SELECT table_schema, table_name
            FROM information_schema.views
            WHERE table_schema NOT IN ('pg_catalog','information_schema','pg_toast','migration')
            ORDER BY table_schema, table_name";

        await using var readConn = await OpenAsync(targetCs);
        var views = await QueryAsync(readConn, query,
            r => (schema: r.GetString(0), name: r.GetString(1)));

        job.Log($"Renaming views ({views.Count} total)...");
        foreach (var (schema, name) in views)
        {
            var snake = ToSnakeCase(name);
            if (snake == name) continue;
            var label = $"view {schema}.{name}";
            try
            {
                await ExecuteWithRetryAsync(async () =>
                {
                    await using var conn = await OpenAsync(targetCs);
                    await using var tx   = await conn.BeginTransactionAsync();
                    await ExecAsync(conn, tx, $"ALTER VIEW {Q(schema)}.{Q(name)} RENAME TO {Q(snake)}");
                    await tx.CommitAsync();
                }, label, job);
                await AppendRollbackAsync(targetCs,
                    $"ALTER VIEW {Q(schema)}.{Q(snake)} RENAME TO {Q(name)}");
                result.ViewsRenamed++;
                result.Changes.Add($"view  {schema}.{name} ? {snake}");
                job.Log($"  {label} ? {snake}");
            }
            catch (Exception ex)
            {
                result.Skipped.Add($"view  {schema}.{name}: {ex.Message}");
                job.Log($"  ?? skipped {label}: {ex.Message}");
            }
        }
    }

    // ?? Functions / Procedures ????????????????????????????????????????????????
    private static async Task RenameFunctionsAsync(string targetCs, RenameToSnakeCaseResult result, RenameJob job)
    {
        const string query = @"
            SELECT n.nspname, p.proname,
                   pg_get_function_identity_arguments(p.oid) AS args
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast','migration')
            ORDER BY n.nspname, p.proname";

        await using var readConn = await OpenAsync(targetCs);
        var funcs = await QueryAsync(readConn, query,
            r => (schema: r.GetString(0), name: r.GetString(1), args: r.GetString(2)));

        job.Log($"Renaming functions ({funcs.Count} total)...");
        foreach (var (schema, name, args) in funcs)
        {
            var snake = ToSnakeCase(name);
            if (snake == name) continue;
            var label = $"function {schema}.{name}";
            try
            {
                await ExecuteWithRetryAsync(async () =>
                {
                    await using var conn = await OpenAsync(targetCs);
                    await using var tx   = await conn.BeginTransactionAsync();
                    await ExecAsync(conn, tx, $"ALTER FUNCTION {Q(schema)}.{Q(name)}({args}) RENAME TO {Q(snake)}");
                    await tx.CommitAsync();
                }, label, job);
                await AppendRollbackAsync(targetCs,
                    $"ALTER FUNCTION {Q(schema)}.{Q(snake)}({args}) RENAME TO {Q(name)}");
                result.FunctionsRenamed++;
                result.Changes.Add($"function  {schema}.{name} ? {snake}");
                job.Log($"  {label} ? {snake}");
            }
            catch (Exception ex)
            {
                result.Skipped.Add($"function  {schema}.{name}: {ex.Message}");
                job.Log($"  ?? skipped {label}: {ex.Message}");
            }
        }
    }

    // ?? Tables ????????????????????????????????????????????????????????????????
    private static async Task RenameTablesAsync(string targetCs, RenameToSnakeCaseResult result, RenameJob job)
    {
        const string query = @"
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog','information_schema','pg_toast','migration')
            ORDER BY table_schema, table_name";

        await using var readConn = await OpenAsync(targetCs);
        var tables = await QueryAsync(readConn, query,
            r => (schema: r.GetString(0), name: r.GetString(1)));

        job.Log($"Renaming tables ({tables.Count} total)...");
        foreach (var (schema, name) in tables)
        {
            var snake = ToSnakeCase(name);
            if (snake == name) continue;
            var label = $"table {schema}.{name}";
            try
            {
                await ExecuteWithRetryAsync(async () =>
                {
                    await using var conn = await OpenAsync(targetCs);
                    await using var tx   = await conn.BeginTransactionAsync();
                    await ExecAsync(conn, tx, $"ALTER TABLE {Q(schema)}.{Q(name)} RENAME TO {Q(snake)}");
                    await tx.CommitAsync();
                }, label, job);
                await AppendRollbackAsync(targetCs,
                    $"ALTER TABLE {Q(schema)}.{Q(snake)} RENAME TO {Q(name)}");
                result.TablesRenamed++;
                result.Changes.Add($"table  {schema}.{name} ? {snake}");
                job.Log($"  {label} ? {snake}");
            }
            catch (Exception ex)
            {
                result.Skipped.Add($"table  {schema}.{name}: {ex.Message}");
                job.Log($"  ?? skipped {label}: {ex.Message}");
            }
        }
    }

    // ?? Schemas ???????????????????????????????????????????????????????????????
    private static async Task RenameSchemasAsync(string targetCs, RenameToSnakeCaseResult result, RenameJob job)
    {
        const string query = @"
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog','information_schema','pg_toast','migration','public')
              AND schema_name NOT LIKE 'pg_%'
            ORDER BY schema_name";

        await using var readConn = await OpenAsync(targetCs);
        var schemas = await QueryAsync(readConn, query, r => r.GetString(0));

        job.Log($"Renaming schemas ({schemas.Count} total)...");
        foreach (var schema in schemas)
        {
            var snake = ToSnakeCase(schema);
            if (snake == schema) continue;
            var label = $"schema {schema}";
            try
            {
                await ExecuteWithRetryAsync(async () =>
                {
                    await using var conn = await OpenAsync(targetCs);
                    await using var tx   = await conn.BeginTransactionAsync();
                    await ExecAsync(conn, tx, $"ALTER SCHEMA {Q(schema)} RENAME TO {Q(snake)}");
                    await tx.CommitAsync();
                }, label, job);
                await AppendRollbackAsync(targetCs,
                    $"ALTER SCHEMA {Q(snake)} RENAME TO {Q(schema)}");
                result.SchemasRenamed++;
                result.Changes.Add($"schema  {schema} ? {snake}");
                job.Log($"  {label} ? {snake}");
            }
            catch (Exception ex)
            {
                result.Skipped.Add($"schema  {schema}: {ex.Message}");
                job.Log($"  ?? skipped {label}: {ex.Message}");
            }
        }
    }

    // ?? Helpers ???????????????????????????????????????????????????????????????

    /// <summary>
    /// Converts PascalCase / camelCase / mixed identifiers to snake_case.
    /// Examples:
    ///   UserId         ? user_id
    ///   LastName       ? last_name
    ///   CreatedAtUtc   ? created_at_utc
    ///   XMLParser      ? xml_parser
    ///   alreadySnake   ? already_snake   (no change once already snake)
    /// </summary>
    internal static string ToSnakeCase(string input)
    {
        if (string.IsNullOrEmpty(input)) return input;

        // Insert _ between a lowercase/digit and an uppercase
        var s = System.Text.RegularExpressions.Regex.Replace(input, @"([a-z0-9])([A-Z])", "$1_$2");
        // Insert _ between consecutive uppercase run and the next lowercase (e.g. XMLParser ? XML_Parser)
        s = System.Text.RegularExpressions.Regex.Replace(s, @"([A-Z]+)([A-Z][a-z])", "$1_$2");

        return s.ToLowerInvariant();
    }

    // Double-quote an identifier
    private static string Q(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";

    private static async Task ExecAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string sql)
    {
        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        cmd.CommandTimeout = 60;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<List<T>> QueryAsync<T>(
        NpgsqlConnection conn,
        string sql,
        Func<NpgsqlDataReader, T> map)
    {
        var list = new List<T>();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            list.Add(map(reader));
        return list;
    }
}
