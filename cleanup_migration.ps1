# PowerShell script to clean up stuck migration database
# Usage: .\cleanup_migration.ps1 -DatabaseName "your_target_db" -PostgresPassword "your_password"

param(
    [Parameter(Mandatory=$true)]
    [string]$DatabaseName,

    [Parameter(Mandatory=$false)]
    [string]$PostgresHost = "localhost",

    [Parameter(Mandatory=$false)]
    [string]$PostgresUser = "postgres",

    [Parameter(Mandatory=$false)]
    [string]$PostgresPassword = "",

    [Parameter(Mandatory=$false)]
    [int]$PostgresPort = 5432
)

Write-Host "?? Cleaning up migration database: $DatabaseName" -ForegroundColor Cyan
Write-Host ""

# Set password environment variable if provided
if ($PostgresPassword) {
    $env:PGPASSWORD = $PostgresPassword
}

# Check if psql is available
try {
    $psqlVersion = psql --version 2>&1
    Write-Host "? PostgreSQL client found: $psqlVersion" -ForegroundColor Green
} catch {
    Write-Host "? Error: psql command not found. Please install PostgreSQL client tools or add them to PATH." -ForegroundColor Red
    Write-Host "   Download from: https://www.postgresql.org/download/" -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "Step 1: Terminating active connections to database..." -ForegroundColor Yellow

$terminateQuery = @"
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '$DatabaseName' AND pid <> pg_backend_pid();
"@

try {
    $result = psql -h $PostgresHost -p $PostgresPort -U $PostgresUser -d postgres -c $terminateQuery 2>&1
    Write-Host "? Connections terminated" -ForegroundColor Green
} catch {
    Write-Host "??  Warning: Could not terminate connections (database may not exist)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Step 2: Dropping database..." -ForegroundColor Yellow

$dropQuery = "DROP DATABASE IF EXISTS `"$DatabaseName`";"

try {
    $result = psql -h $PostgresHost -p $PostgresPort -U $PostgresUser -d postgres -c $dropQuery 2>&1

    if ($LASTEXITCODE -eq 0) {
        Write-Host "? Database '$DatabaseName' dropped successfully" -ForegroundColor Green
    } else {
        Write-Host "??  Warning: $result" -ForegroundColor Yellow
    }
} catch {
    Write-Host "? Error dropping database: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "? Cleanup complete! You can now restart the migration." -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Restart your application (Press F5 in Visual Studio)" -ForegroundColor White
Write-Host "  2. Run the migration again" -ForegroundColor White
Write-Host "  3. Watch for progress updates like: 'Copying data: TableName (1000 rows)'" -ForegroundColor White
Write-Host ""

# Clear password from environment
if ($PostgresPassword) {
    $env:PGPASSWORD = ""
}
