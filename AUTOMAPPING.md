# SmartBCP Automatic Table Mapping

This document explains how to use SmartBCP's automatic table mapping features to efficiently transfer data between databases with many tables across different schemas.

## The Challenge

When working with large databases containing hundreds of tables across multiple schemas, manually listing each table or even each schema in the configuration file becomes impractical. SmartBCP provides several ways to automatically map tables between source and destination databases.

## Automapping Solutions

### 1. Schema-Level Wildcards

The basic configuration supports schema-level wildcards, where you can specify all tables within a particular schema:

```json
"tables": [
    "dbo.*",
    "Sales.*",
    "HR.*"
]
```

This will include all tables from the specified schemas, but you still need to list each schema explicitly.

### 2. ALL Tables Across ALL Schemas

For complete database transfers, you can use the special "ALL" value in the tables array:

```json
"tables": [
    "ALL"
]
```

This special value tells SmartBCP to:
1. Query the source database for all available schemas
2. For each schema, include all tables
3. Process all tables in the correct dependency order

This is ideal for:
- Full database migrations
- Creating complete test/development environments
- Disaster recovery scenarios

## Using the Enhanced Configuration Module

To use the automatic schema discovery feature:

1. Use the enhanced configuration module:
   ```powershell
   # In SmartBCP.ps1, replace:
   Import-Module -Name (Join-Path -Path $modulePath -ChildPath "Configuration.psm1") -Force
   
   # With:
   Import-Module -Name (Join-Path -Path $modulePath -ChildPath "Configuration-Enhanced.psm1") -Force
   ```

2. Create a configuration file with the "ALL" special value:
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

3. Run SmartBCP with this configuration:
   ```powershell
   .\SmartBCP.ps1 -ConfigFile .\Config\sample-allschemas.json
   ```

## How It Works

When SmartBCP encounters the "ALL" special value:

1. It queries `sys.tables` to get all schema names in the source database
2. For each schema, it queries all table names
3. It builds a complete list of fully qualified table names (schema.table)
4. It determines dependencies between tables to establish the correct processing order
5. It processes all tables in parallel, respecting dependencies

## Performance Considerations

When using the "ALL" option:

- Initial schema and table discovery may take longer for very large databases
- Consider increasing `maxThreads` for databases with many tables
- The utility will still handle foreign key constraints correctly
- Tables are processed in the optimal order based on dependencies

## Filtering Options

If you need to transfer most but not all tables, you can combine approaches:

```json
"tables": [
    "ALL",
    "!dbo.AuditLog",
    "!dbo.TemporaryData"
]
```

> Note: The exclusion feature with "!" prefix is not implemented in the current version but could be added as a future enhancement.

## Conclusion

The automatic table mapping feature with the "ALL" special value makes SmartBCP ideal for working with large databases containing hundreds of tables across multiple schemas, eliminating the need to manually specify each table or schema.
