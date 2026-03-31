namespace Migratesqltopostgresql.Web.Models;

public sealed class DatabaseComparisonRequest
{
    public string SourceDbName { get; set; } = string.Empty;
    public string TargetDbName { get; set; } = string.Empty;
    public string SqlServerConnectionTemplate { get; set; } = string.Empty;
    public string PostgresAdminConnection { get; set; } = string.Empty;
}

public sealed class DatabaseComparisonResult
{
    public string SourceDatabase { get; set; } = string.Empty;
    public string TargetDatabase { get; set; } = string.Empty;
    public int SqlServerTableCount { get; set; }
    public int PostgresTableCount { get; set; }
    public List<SchemaComparison> Schemas { get; set; } = new();
    public List<TableComparison> Tables { get; set; } = new();
    public List<string> MissingInPostgres { get; set; } = new();
    public List<string> ExtraInPostgres { get; set; } = new();
}

public sealed class SchemaComparison
{
    public string SchemaName { get; set; } = string.Empty;
    public int SqlServerTables { get; set; }
    public int PostgresTables { get; set; }
}

public sealed class TableComparison
{
    public string Schema { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public long SqlServerRows { get; set; }
    public long PostgresRows { get; set; }
    public bool RowCountMatch => SqlServerRows == PostgresRows;
    public bool ExistsInSqlServer { get; set; } = true;
    public bool ExistsInPostgres { get; set; } = true;
}
