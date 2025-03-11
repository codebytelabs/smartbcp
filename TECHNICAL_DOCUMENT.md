# SmartBCP Technical Documentation

This document provides technical details about the SmartBCP utility, its architecture, design decisions, and implementation details.

## Architecture Overview

SmartBCP is designed with a modular architecture to improve maintainability, readability, and extensibility. The codebase is organized into the following components:

1. **Main Script (SmartBCP.ps1)**: Orchestrates the migration process by importing modules and calling their functions.
2. **Module System**: Separates functionality into logical units:
   - **Logging**: Handles log file creation and message logging
   - **Configuration**: Manages parameter validation and environment variables
   - **Database Operations**: Provides database connectivity and table operations
   - **Foreign Key Management**: Handles foreign key constraints
   - **BCP Operations**: Manages BCP export and import operations
   - **Parallel Processing**: Implements parallel table migration

## Module Details

### Logging Module (Logging.psm1)

The Logging module provides centralized logging functionality with the following features:

- Log initialization with timestamp-based filenames
- Multi-level logging (INFO, WARNING, ERROR, DEBUG, VERBOSE)
- Color-coded console output based on log level
- Retry logic for log file writing to handle transient file access issues
- Global log file path management

Key functions:
- `Initialize-Logging`: Sets up the log file and directory
- `Write-Log`: Writes messages to both console and log file

### Configuration Module (Configuration.psm1)

The Configuration module handles parameter validation and environment variable management:

- Parameter validation to ensure required parameters are provided
- Configuration file loading (JSON format)
- Environment variable loading from .env files
- SQL credential creation from environment variables

Key functions:
- `Load-Configuration`: Loads configuration from a JSON file
- `Validate-Parameters`: Validates required parameters
- `Load-EnvFile`: Loads environment variables from .env file
- `Get-SqlCredentialFromEnv`: Creates SQL credentials from environment variables

### Database Operations Module (DatabaseOperations.psm1)

The Database Operations module provides database connectivity and table operations:

- Table retrieval with filtering
- Table truncation with identity reset
- Database connection testing

Key functions:
- `Get-Tables`: Retrieves tables from a database with filtering
- `Truncate-Table`: Truncates a table and optionally resets identity
- `Test-DatabaseConnection`: Tests database connectivity

### Foreign Key Management Module (ForeignKeyManagement.psm1)

The Foreign Key Management module handles foreign key constraints:

- Foreign key constraint backup
- Foreign key constraint dropping
- Foreign key constraint restoration

Key functions:
- `Backup-ForeignKeyConstraints`: Backs up foreign key constraints to SQL scripts
- `Drop-ForeignKeyConstraints`: Drops foreign key constraints
- `Restore-ForeignKeyConstraints`: Restores foreign key constraints from backup

### BCP Operations Module (BCPOperations.psm1)

The BCP Operations module manages BCP export and import operations:

- BCP command construction
- BCP execution with error handling
- BCP output parsing
- Temporary file management

Key functions:
- `Start-TableMigration`: Migrates a single table using BCP
- `Test-BCPAvailability`: Tests BCP command availability

### Parallel Processing Module (ParallelProcessing.psm1)

The Parallel Processing module implements parallel table migration:

- Background job management
- Progress tracking
- Result aggregation
- Summary reporting

Key functions:
- `Start-ParallelTableMigration`: Migrates multiple tables in parallel

## Workflow

The SmartBCP utility follows this workflow:

1. **Initialization**:
   - Import modules
   - Initialize logging
   - Validate parameters
   - Load environment variables

2. **Pre-migration Checks**:
   - Test database connections
   - Test BCP availability
   - Create and validate temp directory

3. **Table Discovery**:
   - Retrieve tables from source database
   - Apply schema and table filters

4. **Foreign Key Management (Optional)**:
   - Backup foreign key constraints from source
   - Drop foreign key constraints on target

5. **Table Preparation (Optional)**:
   - Truncate target tables

6. **Data Migration**:
   - Initialize migration parameters
   - Start parallel table migration
   - Monitor progress

7. **Post-migration**:
   - Restore foreign key constraints (if backed up)
   - Generate migration summary

## Design Decisions

### Modular Architecture

The decision to use a modular architecture was made to:
- Improve code organization and readability
- Enable easier maintenance and updates
- Allow for better unit testing
- Facilitate code reuse

### PowerShell Modules

PowerShell modules (.psm1) were chosen over script files (.ps1) to:
- Provide proper encapsulation
- Control function exports
- Enable module importing with Import-Module
- Support versioning

### Parallel Processing

Parallel processing was implemented using PowerShell background jobs to:
- Improve migration performance
- Utilize available system resources
- Handle large databases efficiently

### Error Handling

Comprehensive error handling was implemented to:
- Provide detailed error information
- Enable troubleshooting
- Prevent data corruption
- Allow for recovery

### Logging

Detailed logging was implemented to:
- Track migration progress
- Provide audit trail
- Enable troubleshooting
- Report migration statistics

## Performance Considerations

- **Parallel Processing**: The number of parallel tasks can be adjusted based on system resources
- **BCP Format**: Native format is fastest but less portable
- **Batch Size**: Can be adjusted for optimal performance through the batchSize configuration parameter
- **Foreign Key Constraints**: Disabling constraints improves import performance

## Security Considerations

- **SQL Authentication**: Credentials can be provided via parameters or environment variables
- **Windows Authentication**: Used by default if SQL credentials are not provided
- **Password Masking**: Passwords are masked in log files
- **Secure String**: Passwords are stored as secure strings in memory

## Extension Points

The modular architecture allows for easy extension:

- **New BCP Formats**: Add support for additional BCP formats
- **Additional Filters**: Implement more sophisticated table filtering
- **Schema Comparison**: Add schema comparison before migration
- **Data Transformation**: Add data transformation during migration
- **Progress Reporting**: Enhance progress reporting with UI elements

## Troubleshooting

- **Log Files**: Check log files in the Logs directory
- **Temporary Files**: Examine BCP files in the Temp directory
- **Error Messages**: Review error messages in the log files
- **Database Errors**: Check SQL Server error logs

## Future Improvements

- **Schema Creation**: Add support for creating schema in target database
- **Data Comparison**: Add data comparison after migration
- **Incremental Migration**: Support for incremental data migration
- **GUI Interface**: Add graphical user interface
- **Scheduled Migration**: Support for scheduled migration jobs
