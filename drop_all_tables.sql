USE [AdventureWorks_Copy];

PRINT '-- Dropping all foreign key constraints';
DECLARE @DropFKConstraint NVARCHAR(MAX);
DECLARE FKCursor CURSOR FOR
    SELECT 'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] DROP CONSTRAINT [' + name + '];'
    FROM sys.foreign_keys;

OPEN FKCursor;
FETCH NEXT FROM FKCursor INTO @DropFKConstraint;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT @DropFKConstraint;
    EXEC sp_executesql @DropFKConstraint;
    FETCH NEXT FROM FKCursor INTO @DropFKConstraint;
END;

CLOSE FKCursor;
DEALLOCATE FKCursor;

PRINT '-- Dropping all tables';
DECLARE @DropTable NVARCHAR(MAX);
DECLARE TableCursor CURSOR FOR
    SELECT 'DROP TABLE [' + s.name + '].[' + t.name + '];'
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.is_ms_shipped = 0
    ORDER BY t.name;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @DropTable;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT @DropTable;
    EXEC sp_executesql @DropTable;
    FETCH NEXT FROM TableCursor INTO @DropTable;
END;

CLOSE TableCursor;
DEALLOCATE TableCursor;

PRINT '-- All foreign key constraints and tables have been dropped';
