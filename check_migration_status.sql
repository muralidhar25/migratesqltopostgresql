-- Check Migration Status and Data in PostgreSQL
-- Run this to see what was migrated before the permission error

-- 1. Check if target database exists
SELECT datname FROM pg_database WHERE datname = 'your_target_database_name';

-- 2. List all schemas (should see Auth, Mst, Tenant, migration)
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY schema_name;

-- 3. List all tables across all schemas
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, tablename;

-- 4. Count rows in each table (to see if data was actually copied)
SELECT 
    schemaname,
    tablename,
    (SELECT COUNT(*) FROM (SELECT schemaname||'.'||tablename) t) as row_count
FROM pg_tables 
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, tablename;

-- 5. Check for the migration metadata table
SELECT * FROM information_schema.tables 
WHERE table_schema = 'migration' 
  AND table_name = 'migration_source_objects';

-- 6. If you know your target database name, connect to it and run:
-- \c your_target_database_name

-- Then check for specific tables
SELECT 
    table_schema,
    table_name,
    (xpath('/row/cnt/text()', 
           query_to_xml(format('SELECT COUNT(*) as cnt FROM %I.%I', 
                              table_schema, table_name), 
                        false, true, '')))[1]::text::int as row_count
FROM information_schema.tables
WHERE table_schema IN ('Auth', 'Mst', 'Tenant', 'dbo')
ORDER BY table_schema, table_name;
