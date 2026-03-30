# migratesqltopostgresql

Simple local ASP.NET Core MVC app (.NET 10) to migrate SQL Server databases to PostgreSQL.

## Features

- Input box for source DB name and `Migrate` button.
- Optional target DB name input (defaults to source name).
- Fixed, scrollable migration log/status panel.
- Progress bar with live polling.
- Transactional migration for schema + data in PostgreSQL.
- Rollback on error (transaction rollback; also drops newly created target DB).
- Captures SQL Server functions, procedures, and views into PostgreSQL for manual conversion.

## Project

- MVC app: `Migratesqltopostgresql.Web`

## Configure connection strings

Edit `Migratesqltopostgresql.Web/appsettings.json`:

- `Migration:SqlServerConnectionTemplate`
	- Must include `{dbname}` placeholder.
	- Example:
		- `Server=localhost;Database={dbname};User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=True;Encrypt=False;`
- `Migration:PostgresAdminConnection`
	- Example:
		- `Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=postgres;`
- `Migration:BatchSize`
	- Row copy batch size.

## Run locally

```bash
cd Migratesqltopostgresql.Web
dotnet restore
dotnet run
```

Open `http://localhost:5000` or the URL shown in terminal.

## Migration behavior and safety

1. Verifies source/target DB names.
2. Connects to SQL Server using fixed template.
3. Creates target PostgreSQL DB if missing.
4. Opens one serializable transaction for target schema + data migration.
5. Applies advisory lock to avoid concurrent migration overlap.
6. Recreates tables and copies all table rows.
7. Stores routine/view source in `public.migration_source_objects`.
8. Commits only after schema + data complete.
9. On any error, rolls back transaction and reports error to UI.
10. If DB was newly created in this run, drops it after failure.

## Notes

- SQL Server stored procedures/functions are captured for conversion, not auto-translated to PostgreSQL procedural SQL.
- Complex SQL Server-specific types or logic may require post-migration validation.
