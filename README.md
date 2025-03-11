# SmartBCP - SQL Server Database Migration Utility

SmartBCP is a PowerShell utility for efficient SQL Server database migration using native BCP (Bulk Copy Program). It performs parallel data migration between SQL Server databases with identical schema, handling foreign key constraints, optimizing performance, and managing temporary resources.

## Features

- Parallel table migration for improved performance
- Foreign key constraint management (backup, drop, and restore)
- Flexible table filtering (include/exclude schemas and tables)
- Support for both Windows and SQL authentication
- Detailed logging and progress tracking
- Temporary file management
- Error handling and recovery

## Requirements

- PowerShell 5.1 or later
- SQL Server Management Objects (SMO)
- BCP utility installed and available in PATH
- Source and target SQL Server instances with identical schema

## Usage

```powershell
.\SmartBCP.ps1 -SourceServer "SourceSQLServer" -SourceDB "AdventureWorks" -TargetServer "TargetSQLServer" -TargetDB "AdventureWorks_Copy" -ParallelTasks 8
```

## Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| SourceServer | Source SQL Server instance name | (Required) |
| SourceDB | Source database name | (Required) |
| TargetServer | Target SQL Server instance name | (Required) |
| TargetDB | Target database name | (Required) |
| IncludeSchemas | Schemas to include in migration | (All schemas) |
| ExcludeSchemas | Schemas to exclude from migration | (None) |
| IncludeTables | Tables to include in migration | (All tables) |
| ExcludeTables | Tables to exclude from migration | (None) |
| TruncateTargetTables | Whether to truncate target tables before migration | $false |
| ManageForeignKeys | Whether to handle foreign key constraints | $false |
| ParallelTasks | Number of tables to migrate in parallel | 4 |
| BCPFormat | BCP format to use: native, char, or widechar | native |
| TempPath | Path for temporary files | .\Temp |
| BatchSize | Number of rows to process in a single batch during import | 10000 |
| SourceCredential | SQL credentials for source database connection | (Windows auth) |
| TargetCredential | SQL credentials for target database connection | (Windows auth) |

## Environment Variables

You can also set SQL authentication credentials using environment variables in a `.env` file:

```
sql_user=username
sql_password=password
```

## Project Structure

The project is organized into modules for better maintainability:

- **SmartBCP.ps1** - Main script that orchestrates the migration process
- **Modules/**
  - **Logging.psm1** - Logging functions
  - **Configuration.psm1** - Configuration and parameter handling
  - **DatabaseOperations.psm1** - Database-related functions
  - **ForeignKeyManagement.psm1** - Foreign key constraint management
  - **BCPOperations.psm1** - BCP migration operations
  - **ParallelProcessing.psm1** - Parallel processing functions

## Examples

### Basic Migration

```powershell
.\SmartBCP.ps1 -SourceServer "Server1" -SourceDB "DB1" -TargetServer "Server2" -TargetDB "DB2"
```

### Migration with Foreign Key Management

```powershell
.\SmartBCP.ps1 -SourceServer "Server1" -SourceDB "DB1" -TargetServer "Server2" -TargetDB "DB2" -ManageForeignKeys
```

### Migration with Table Filtering

```powershell
.\SmartBCP.ps1 -SourceServer "Server1" -SourceDB "DB1" -TargetServer "Server2" -TargetDB "DB2" -IncludeSchemas "dbo","sales" -ExcludeTables "Logs","Audit"
```

### Migration with SQL Authentication

```powershell
$sourceCred = Get-Credential -Message "Enter source database credentials"
$targetCred = Get-Credential -Message "Enter target database credentials"
.\SmartBCP.ps1 -SourceServer "Server1" -SourceDB "DB1" -TargetServer "Server2" -TargetDB "DB2" -SourceCredential $sourceCred -TargetCredential $targetCred
```

### Migration Using Configuration File

```powershell
.\SmartBCP.ps1 -ConfigFile .\test-sql-auth.json
```

This approach uses a JSON configuration file that contains all the necessary parameters. Example configuration files are provided in the repository (config-sql-auth.json, config-windows-auth.json).

## Logs

Logs are stored in the `Logs` directory with timestamps in the filename. Each log entry includes a timestamp, level, and message.
