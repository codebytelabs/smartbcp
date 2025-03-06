# Smart BCP Utility

A high-performance bulk data transfer utility for SQL Server using native tools.

## Overview

Smart BCP is a PowerShell-based utility that provides high-speed data transfer capabilities between SQL Server databases by leveraging parallel processing and partitioning strategies. Unlike similar tools that rely on .NET Core, Smart BCP uses only native SQL Server and Windows tools such as PowerShell, SQLCMD, and BCP.

## Features

- Configuration-driven operation using JSON files
- Foreign key constraint handling (automatic dropping and recreation)
- Table dependency resolution for proper processing order
- Dynamic partitioning for parallel processing of large tables
- Parallel execution using PowerShell jobs
- Comprehensive error handling and logging
- Efficient temporary file management
- Automatic schema discovery and table mapping
- Support for both Windows and SQL Server authentication

## Prerequisites

- Windows operating system
- SQL Server installed (or SQL Server client tools at minimum)
- PowerShell 5.1 or higher
- Appropriate SQL Server permissions on source and destination databases

## Usage

```
.\SmartBCP.ps1 -ConfigFile .\Config\your-config.json [-LogFile .\logs\transfer.log] [-DetailedLogging]
```

### Parameters

- **ConfigFile** (Required): Path to the JSON configuration file
- **LogFile** (Optional): Path to the log file
- **DetailedLogging** (Optional): Enable verbose logging

## Configuration

Smart BCP includes three sample configuration files:

1. **windows-auth-all-tables.json**: Windows authentication with automatic discovery of all tables
2. **sql-auth-all-tables.json**: SQL Server authentication with automatic discovery of all tables
3. **sql-auth-selected-tables.json**: SQL Server authentication with specific tables listed

### Windows Authentication Example

```json
{
    "source": {
        "server": "SourceServer",
        "database": "SourceDatabase",
        "authentication": "windows"
    },
    "destination": {
        "server": "DestinationServer",
        "database": "DestinationDatabase",
        "authentication": "windows"
    },
    "tables": [
        "ALL"
    ],
    "options": {
        "maxThreads": 8,
        "batchSize": 10000,
        "truncateTarget": true,
        "tempFolder": "./Temp"
    }
}
```

### SQL Authentication Example

```json
{
    "source": {
        "server": "SourceServer",
        "database": "SourceDatabase",
        "authentication": "sql",
        "username": "sourceUser",
        "password": "sourcePassword"
    },
    "destination": {
        "server": "DestinationServer",
        "database": "DestinationDatabase",
        "authentication": "sql",
        "username": "destUser",
        "password": "destPassword"
    },
    "tables": [
        "ALL"
    ],
    "options": {
        "maxThreads": 8,
        "batchSize": 10000,
        "truncateTarget": true,
        "tempFolder": "./Temp"
    }
}
```

### Selected Tables Example

```json
{
    "source": {
        "server": "SourceServer",
        "database": "SourceDatabase",
        "authentication": "sql",
        "username": "sourceUser",
        "password": "sourcePassword"
    },
    "destination": {
        "server": "DestinationServer",
        "database": "DestinationDatabase",
        "authentication": "sql",
        "username": "destUser",
        "password": "destPassword"
    },
    "tables": [
        "dbo.Customers",
        "dbo.Orders",
        "dbo.OrderDetails",
        "Sales.SalesPersons"
    ],
    "options": {
        "maxThreads": 8,
        "batchSize": 10000,
        "truncateTarget": true,
        "tempFolder": "./Temp"
    }
}
```

### Configuration Options

- **source**: Source database connection details
  - **server**: SQL Server instance name
  - **database**: Database name
  - **authentication**: Authentication type ("windows" or "sql")
  - **username**: SQL login username (required for SQL authentication)
  - **password**: SQL login password (required for SQL authentication)
  
- **destination**: Destination database connection details (same structure as source)

- **tables**: List of tables to process
  - Specific tables: "schema.table"
  - Schema wildcards: "schema.*"
  - All tables: "ALL" (requires Configuration-Enhanced.psm1)

- **options**:
  - **maxThreads**: Maximum number of parallel threads (default: 8)
  - **batchSize**: Number of rows per batch for BCP import (default: 10000)
  - **truncateTarget**: Whether to truncate target tables before import (default: true)
  - **tempFolder**: Folder for temporary files (default: %TEMP%\SmartBCP)

## Automatic Schema Discovery

To use the automatic schema discovery feature with the "ALL" special value:

1. Make sure you're using the enhanced configuration module:
   ```powershell
   # In SmartBCP.ps1, replace:
   Import-Module -Name (Join-Path -Path $modulePath -ChildPath "Configuration.psm1") -Force
   
   # With:
   Import-Module -Name (Join-Path -Path $modulePath -ChildPath "Configuration-Enhanced.psm1") -Force
   ```

2. Use a configuration file with the "ALL" special value in the tables array.

For more details on automatic schema discovery, see [AUTOMAPPING.md](AUTOMAPPING.md).

## License

MIT License
