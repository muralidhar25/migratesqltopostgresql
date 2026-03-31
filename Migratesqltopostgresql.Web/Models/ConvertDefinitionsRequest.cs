namespace Migratesqltopostgresql.Web.Models;

public sealed class ConvertDefinitionsRequest
{
    public string TargetDbName { get; set; } = string.Empty;
    public string PostgresAdminConnection { get; set; } = string.Empty;
}
