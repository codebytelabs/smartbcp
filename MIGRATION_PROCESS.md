# SmartBCP Migration Process: Step-by-Step

This document provides a detailed, step-by-step explanation of the entire migration process when using SmartBCP.

## 1. Initialization Phase

### 1.1. Script Invocation
- User invokes SmartBCP.ps1 with required parameters
- PowerShell begins script execution
- Script parameters are parsed and stored

### 1.2. Module Loading
- The script identifies the Modules directory path
- Each module is imported using Import-Module with -Force flag:
  - Logging.psm1
  - Configuration.psm1
  - DatabaseOperations.psm1
  - ForeignKeyManagement.psm1
  - BCPOperations.psm1
  - ParallelProcessing.psm1

### 1.3. Environment Variable Loading
- The script checks for a .env file in the current directory
- If found, the file is parsed line by line
- Environment variables are extracted using regex pattern matching
- Variables are set in the current process environment
- SQL credentials are created from environment variables if available

### 1.4. Logging Initialization
- A timestamp is generated for the log filename
- The Logs directory is created if it doesn't exist
- A log file is created with the naming pattern "SmartBCP_[timestamp].log"
- An initial log entry is written to verify file access
- The global LogFilePath variable is set for use throughout the script

## 2. Validation Phase

### 2.1. Parameter Validation
- Required parameters are checked (SourceServer, SourceDB, TargetServer, TargetDB)
- If any required parameter is missing, an error is logged and execution stops
- Parameter types and values are validated where applicable

### 2.2. Directory Validation
- The temporary directory path is resolved to an absolute path
- The temporary directory is created if it doesn't exist
- A test file is written to verify write access to the temporary directory
- The test file is deleted after successful verification
- The log directory is verified to exist

### 2.3. Database Connection Testing
- Source database connection is tested:
  - A simple query is executed to retrieve version, database name, and current time
  - If using SQL authentication, credentials are applied
  - Connection success/failure is logged
  - Version information is logged on success
- Target database connection is tested:
  - Same process as source database
  - Connection success/failure is logged
  - Version information is logged on success

### 2.4. BCP Utility Verification
- The BCP command is executed with version flag (-v)
- Output is captured and exit code is checked
- If BCP is available, version information is logged
- If BCP is not available or returns an error, execution stops

## 3. Table Discovery Phase

### 3.1. Table Retrieval
- A SQL query is constructed to retrieve table information from the source database
- The query includes:
  - TABLE_SCHEMA and TABLE_NAME
  - TABLE_TYPE (filtered to BASE TABLE only)
  - TableObjectID for internal reference
  - HasPrimaryKey flag
  - HasIdentity flag
- The query excludes system tables and applies schema/table filters
- The query is executed against the source database
- Results are stored in a tables collection

### 3.2. Table Filtering
- If IncludeSchemas is specified, only tables in those schemas are included
- If ExcludeSchemas is specified, tables in those schemas are excluded
- If IncludeTables is specified, only those tables are included
- If ExcludeTables is specified, those tables are excluded
- The final filtered table list is logged

## 4. Foreign Key Management Phase (Optional)

### 4.1. Foreign Key Backup
- If ManageForeignKeys switch is enabled:
  - A timestamp is generated for backup files
  - A backup directory is created if needed
  - A SQL query retrieves all foreign key constraints from source database
  - For each constraint, CREATE and DROP scripts are generated
  - Scripts are saved to separate files:
    - ForeignKeyDrops.sql: Contains all DROP CONSTRAINT statements
    - ForeignKeyCreates.sql: Contains all ADD CONSTRAINT statements
  - The number of backed up constraints is logged

### 4.2. Foreign Key Dropping
- If ManageForeignKeys switch is enabled:
  - A SQL query retrieves all foreign key constraints from target database
  - For each constraint, a DROP CONSTRAINT statement is generated
  - Each DROP statement is executed individually
  - Success/failure of each statement is logged
  - Overall success is logged

## 5. Table Preparation Phase (Optional)

### 5.1. Table Truncation
- If TruncateTargetTables switch is enabled:
  - For each table in the filtered list:
    - A TRUNCATE TABLE statement is constructed
    - If the table has an identity column, a DBCC CHECKIDENT statement is added
    - The statement is executed against the target database
    - If truncation fails (e.g., due to foreign key constraints):
      - A DELETE FROM statement is attempted as fallback
      - Success/failure is logged

## 6. Migration Preparation Phase

### 6.1. Migration Parameters Setup
- A hashtable of migration parameters is created containing:
  - SourceServer: Source SQL Server instance
  - SourceDB: Source database name
  - TargetServer: Target SQL Server instance
  - TargetDB: Target database name
  - TempFilePath: Path for temporary BCP files
  - BCPFormat: Format for BCP operations (native, char, widechar)
  - BatchSize: Number of rows to process in a single batch during import
  - SourceCredential: Credentials for source database
  - TargetCredential: Credentials for target database

## 7. Parallel Migration Phase

### 7.1. Parallel Migration Initialization
- Progress tracking variables are initialized:
  - totalTables: Total number of tables to migrate
  - completedTables: Counter for completed tables
  - successfulTables: Counter for successfully migrated tables
  - failedTables: Counter for failed migrations
  - results: Collection to store migration results
  - startTime: Start time for the entire migration
- Job tracking collections are initialized:
  - runningJobs: Hashtable to track running jobs
  - pendingTables: Queue of tables pending migration

### 7.2. Job Creation and Execution
- While there are pending tables or running jobs:
  - Start new jobs up to the parallel limit:
    - Dequeue a table from the pending tables
    - Create a scriptblock for the background job
    - Start a background job with a unique name
    - Add job to tracking collection
  - Check for completed jobs:
    - Identify jobs that are no longer running
    - Receive job results
    - Update counters (completedTables, successfulTables, failedTables)
    - Log success or failure
    - Add results to results collection
    - Remove job from tracking collection
    - Update progress bar
  - Sleep briefly to prevent CPU thrashing

### 7.3. Individual Table Migration Process (For Each Table)
- Create a temporary file path for the table
- Ensure the temporary directory exists
- Build BCP export command with appropriate parameters
- Test connection to source database
- Execute BCP export command and capture output
- Verify export success and temporary file creation
- Build BCP import command with appropriate parameters (including batch size)
- Test connection to target database
- Execute BCP import command with batch size parameter (-b flag) and capture output
- Parse row count from BCP output
- Record success or failure
- Calculate duration
- Return detailed result object

### 7.4. Migration Completion
- Calculate total duration
- Generate summary statistics:
  - Total tables processed
  - Successful migrations
  - Failed migrations
  - Total rows migrated
  - Average time per table
- Log summary information
- Log detailed results for each table (success/failure, row count, duration)

## 8. Post-Migration Phase

### 8.1. Foreign Key Restoration (Optional)
- If ManageForeignKeys switch was enabled:
  - Read the foreign key creation script
  - Split the script into individual statements
  - Execute each statement individually
  - Log success/failure for each statement
  - Log overall restoration success

### 8.2. Final Summary
- Log migration completion
- Log summary statistics:
  - Total tables processed
  - Successful migrations
  - Failed migrations
- If any tables failed, log a warning
- Return migration results to the caller

## 9. Cleanup Phase

### 9.1. Temporary File Handling
- Temporary BCP files are kept for debugging purposes
- Their locations are logged for reference

### 9.2. Job Cleanup
- All background jobs are removed
- Job resources are released

## 10. Error Handling Throughout

### 10.1. Error Capture and Logging
- All operations are wrapped in try/catch blocks
- Errors are logged with detailed information
- Error stack traces are captured
- Critical errors are thrown to stop execution
- Non-critical errors are handled gracefully when possible

### 10.2. Retry Logic
- Log file writing includes retry logic
- Database operations include error handling
- BCP operations capture and log detailed error information

This detailed process ensures reliable, efficient, and well-documented database migration using the native BCP utility with parallel processing capabilities.
