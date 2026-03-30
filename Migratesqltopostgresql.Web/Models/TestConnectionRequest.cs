namespace Migratesqltopostgresql.Web.Models;

public sealed class TestConnectionRequest
{
    /// <summary>"sqlserver" or "postgres"</summary>
    public string Type { get; set; } = string.Empty;
    public string ConnectionString { get; set; } = string.Empty;
}
