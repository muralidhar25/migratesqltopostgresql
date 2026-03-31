
-- SQL Script to Fix SUSER_NAME() and Other SQL Server Functions in PostgreSQL
-- Run this directly on your PostgreSQL database to update existing migration_source_objects

-- First, add the converted_definition column if it doesn't exist
ALTER TABLE migration.migration_source_objects 
ADD COLUMN IF NOT EXISTS converted_definition TEXT;

-- Update all existing records with converted definitions
UPDATE migration.migration_source_objects
SET converted_definition = 
    -- Replace SQL Server functions with PostgreSQL equivalents
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
        -- Remove square brackets from identifiers
        REGEXP_REPLACE(source_definition, '\[([^\]]+)\]', '\1', 'g'),
        'SUSER_NAME()', 'CURRENT_USER'),
        'SUSER_SNAME()', 'CURRENT_USER'),
        'USER_NAME()', 'CURRENT_USER'),
        'SYSTEM_USER', 'CURRENT_USER'),
        'SYSDATETIME()', 'CURRENT_TIMESTAMP'),
        'SYSUTCDATETIME()', 'CURRENT_TIMESTAMP'),
        'SYSDATETIMEOFFSET()', 'CURRENT_TIMESTAMP'),
        'GETDATE()', 'CURRENT_TIMESTAMP'),
        'GETUTCDATE()', 'CURRENT_TIMESTAMP'),
        'NEWID()', 'gen_random_uuid()'),
        'NEWSEQUENTIALID()', 'gen_random_uuid()'),
        'LEN(', 'LENGTH('),
        'CHARINDEX(', 'POSITION('),
        'ISNULL(', 'COALESCE('),
        '[NVARCHAR]', 'VARCHAR'),
        '[VARCHAR]', 'VARCHAR'),
        '[INT]', 'INTEGER')
WHERE source_definition IS NOT NULL;

-- View the results
SELECT 
    object_schema,
    object_name,
    object_type,
    LEFT(source_definition, 100) as source_preview,
    LEFT(converted_definition, 100) as converted_preview
FROM migration.migration_source_objects
ORDER BY object_schema, object_name;

-- To see specific objects that had SUSER_NAME
SELECT 
    object_schema,
    object_name,
    object_type,
    converted_definition
FROM migration.migration_source_objects
WHERE source_definition LIKE '%SUSER_NAME%'
ORDER BY object_schema, object_name;
