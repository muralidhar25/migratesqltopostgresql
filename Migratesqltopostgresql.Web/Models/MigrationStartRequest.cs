namespace Migratesqltopostgresql.Web.Models;

public sealed class MigrationStartRequest
{
    public string DbName { get; set; } = string.Empty;
    public string? TargetDbName { get; set; }
    public string? SqlServerConnectionTemplate { get; set; }
    public string? PostgresAdminConnection { get; set; }
    public string MigrationMode { get; set; } = "schemaAndData"; // "schemaOnly" or "schemaAndData"
}
