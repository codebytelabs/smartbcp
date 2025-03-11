# Parallel Processing Module for SmartBCP
# Contains functions for parallel processing of tables

function Start-ParallelTableMigration {
    param (
        [Parameter(Mandatory=$true)]
        [PSObject[]]$Tables,

        [Parameter(Mandatory=$true)]
        [int]$ParallelTasks,

        [Parameter(Mandatory=$true)]
        [hashtable]$MigrationParams
    )

    Write-Log "Starting parallel table migration with $ParallelTasks tasks..." -Level INFO
    Write-Log "Total tables to migrate: $($Tables.Count)" -Level INFO

    # Initialize counters and statistics
    $successfulTables = 0
    $failedTables = 0
    $totalSourceRows = 0
    $totalTargetRows = 0
    $totalDataSizeBytes = 0
    $totalDurationSeconds = 0
    $tableResults = @()
    $startTime = Get-Date

    # Create a runspace pool and open it
    $runspacePool = [runspacefactory]::CreateRunspacePool(1, $ParallelTasks)
    $runspacePool.Open()

    # Create a collection to hold the runspaces
    $runspaces = New-Object System.Collections.ArrayList

    # Create a script block for table migration
    $scriptBlock = {
        param (
            [PSObject]$Table,
            [hashtable]$MigrationParams
        )

        try {
            # Define Write-Log function directly in the runspace
            function Write-Log {
                param(
                    [Parameter(Mandatory=$true)]
                    [string]$Message,
                    
                    [Parameter(Mandatory=$false)]
                    [ValidateSet("INFO", "WARNING", "ERROR", "DEBUG", "VERBOSE")]
                    [string]$Level = "INFO"
                )
                
                $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
                $logMessage = "[$timestamp] [$Level] $Message"
                
                # Write to console based on level
                switch ($Level) {
                    "ERROR" { Write-Output $logMessage }
                    "WARNING" { Write-Output $logMessage }
                    "INFO" { Write-Output $logMessage }
                    "DEBUG" { Write-Output $logMessage }
                    "VERBOSE" { Write-Output $logMessage }
                }
                
                # Write to log file
                try {
                    $logDir = Join-Path $MigrationParams.ModulePath "..\Logs"
                    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
                    $logFilePath = Join-Path $logDir "SmartBCP_${timestamp}_runspace.log"
                    
                    if (-not (Test-Path $logDir)) {
                        New-Item -Path $logDir -ItemType Directory -Force | Out-Null
                    }
                    
                    [System.IO.File]::AppendAllText($logFilePath, "$logMessage`r`n")
                }
                catch {
                    Write-Output "Failed to write to log file: $($_.Exception.Message)"
                }
            }
            
            # Define Export-TableData function directly in the runspace
            function Export-TableData {
                param (
                    [string]$Server,
                    [string]$Database,
                    [string]$Schema,
                    [string]$Table,
                    [string]$OutputFile,
                    [string]$Format = "native",
                    [System.Management.Automation.PSCredential]$Credential
                )
                
                Write-Log "Export-TableData called with parameters:" -Level INFO
                Write-Log "  Server: $Server" -Level INFO
                Write-Log "  Database: $Database" -Level INFO
                Write-Log "  Schema: $Schema" -Level INFO
                Write-Log "  Table: $Table" -Level INFO
                Write-Log "  OutputFile: $OutputFile" -Level INFO
                Write-Log "  Format: $Format" -Level INFO
                
                $fullTableName = "$Schema.$Table"
                $formatOptions = switch ($Format) {
                    "native" { "-n" }
                    "char" { "-c" }
                    "widechar" { "-w" }
                    default { "-n" }
                }
                
                # Verify output directory exists
                $outputDir = Split-Path -Path $OutputFile -Parent
                Write-Log "Output directory: $outputDir" -Level INFO
                
                if (-not (Test-Path $outputDir)) {
                    Write-Log "Output directory does not exist: $outputDir" -Level ERROR
                    Write-Log "Creating output directory..." -Level INFO
                    try {
                        $newDir = New-Item -Path $outputDir -ItemType Directory -Force
                        Write-Log "Created directory: $($newDir.FullName)" -Level INFO
                    } catch {
                        Write-Log "Failed to create output directory: $($_.Exception.Message)" -Level ERROR
                        return $false
                    }
                } else {
                    Write-Log "Output directory exists" -Level INFO
                    
                    # Check directory permissions
                    try {
                        $testFile = Join-Path $outputDir "test_$(Get-Random).tmp"
                        [System.IO.File]::WriteAllText($testFile, "Test")
                        Write-Log "Successfully wrote test file to output directory" -Level INFO
                        Remove-Item $testFile -Force
                    } catch {
                        Write-Log "Failed to write test file to output directory: $($_.Exception.Message)" -Level ERROR
                        Write-Log "This may indicate a permissions issue" -Level ERROR
                    }
                }
                
                $bcpCommand = "bcp `"$fullTableName`" out `"$OutputFile`" $formatOptions -S `"$Server`" -d `"$Database`""
                
                if ($Credential) {
                    $bcpCommand += " -U `"$($Credential.UserName)`" -P `"$($Credential.GetNetworkCredential().Password)`""
                } else {
                    $bcpCommand += " -T"
                }
                
                $bcpCommand += " -t"
                
                # Log the BCP command (without password)
                $logCommand = $bcpCommand
                if ($Credential) {
                    $logCommand = $bcpCommand -replace " -P `"[^`"]+`"", " -P `"********`""
                }
                Write-Log "BCP command: $logCommand" -Level INFO
                
                try {
                    Write-Log "Exporting data from ${fullTableName} to $OutputFile..." -Level INFO
                    
                    # Execute the BCP command
                    $output = Invoke-Expression $bcpCommand -ErrorAction Stop
                    Write-Log "Successfully exported data from ${fullTableName}" -Level INFO
                    Write-Log "BCP output: $output" -Level INFO
                    
                    # Verify the file was created
                    if (Test-Path $OutputFile) {
                        $fileInfo = Get-Item $OutputFile
                        Write-Log "Output file created: $OutputFile, Size: $($fileInfo.Length) bytes" -Level INFO
                    } else {
                        Write-Log "Output file was not created: $OutputFile" -Level ERROR
                        Write-Log "BCP process completed successfully but no output file was created" -Level ERROR
                        return $false
                    }
                    
                    return $true
                } catch {
                    Write-Log ("Error exporting data from ${fullTableName}`: {0}" -f $_.Exception.Message) -Level ERROR
                    return $false
                }
            }
            
            # Define Import-TableData function directly in the runspace
            function Import-TableData {
                param (
                    [string]$Server,
                    [string]$Database,
                    [string]$Schema,
                    [string]$Table,
                    [string]$InputFile,
                    [string]$Format = "native",
                    [System.Management.Automation.PSCredential]$Credential,
                    [int]$BatchSize = 10000
                )
                
                $fullTableName = "$Schema.$Table"
                $formatOptions = switch ($Format) {
                    "native" { "-n" }
                    "char" { "-c" }
                    "widechar" { "-w" }
                    default { "-n" }
                }
                
                # Verify input file exists
                if (-not (Test-Path $InputFile)) {
                    Write-Log "Input file does not exist: $InputFile" -Level ERROR
                    return $false
                }
                
                $fileInfo = Get-Item $InputFile
                Write-Log "Input file: $InputFile, Size: $($fileInfo.Length) bytes" -Level INFO
                
                try {
                    Write-Log "Importing data to ${fullTableName} from $InputFile..." -Level INFO
                    
                    $bcpCommand = "bcp `"$fullTableName`" in `"$InputFile`" $formatOptions -S `"$Server`" -d `"$Database`" -b $BatchSize"
                    
                    if ($Credential) {
                        $bcpCommand += " -U `"$($Credential.UserName)`" -P `"$($Credential.GetNetworkCredential().Password)`""
                    } else {
                        $bcpCommand += " -T"
                    }
                    
                    $bcpCommand += " -t"
                    
                    # Log the BCP command (without password)
                    $logCommand = $bcpCommand
                    if ($Credential) {
                        $logCommand = $bcpCommand -replace " -P `"[^`"]+`"", " -P `"********`""
                    }
                    Write-Log "BCP command: $logCommand" -Level INFO
                    
                    $output = Invoke-Expression $bcpCommand -ErrorAction Stop
                    Write-Log "Successfully imported data to ${fullTableName}" -Level INFO
                    Write-Log "BCP output: $output" -Level INFO
                    return $true
                } catch {
                    Write-Log ("Error importing data to ${fullTableName}`: {0}" -f $_.Exception.Message) -Level ERROR
                    return $false
                }
            }
            
            # Define Migrate-TableData function directly in the runspace
            function Migrate-TableData {
                param (
                    [Parameter(Mandatory=$true)]
                    [PSObject]$Table,
                    
                    [Parameter(Mandatory=$true)]
                    [hashtable]$MigrationParams
                )
                
                Write-Log "Migrate-TableData called with parameters:" -Level INFO
                Write-Log "  Table: $($Table.TABLE_SCHEMA).$($Table.TABLE_NAME)" -Level INFO
                Write-Log "  MigrationParams:" -Level INFO
                Write-Log "    SourceServer: $($MigrationParams.SourceServer)" -Level INFO
                Write-Log "    SourceDB: $($MigrationParams.SourceDB)" -Level INFO
                Write-Log "    TargetServer: $($MigrationParams.TargetServer)" -Level INFO
                Write-Log "    TargetDB: $($MigrationParams.TargetDB)" -Level INFO
                Write-Log "    TempFilePath: $($MigrationParams.TempFilePath)" -Level INFO
                Write-Log "    BCPFormat: $($MigrationParams.BCPFormat)" -Level INFO
                
                $schema = $Table.TABLE_SCHEMA
                $tableName = $Table.TABLE_NAME
                $fullTableName = "$schema.$tableName"
                
                $tempFileName = "$schema`_$tableName`_$(Get-Date -Format 'yyyyMMddHHmmss').dat"
                $tempFilePath = Join-Path $MigrationParams.TempFilePath $tempFileName
                
                Write-Log "Temp file path for ${fullTableName}: $tempFilePath" -Level INFO
                
                # Verify temp directory exists
                if (-not (Test-Path $MigrationParams.TempFilePath)) {
                    Write-Log "Temp directory does not exist: $($MigrationParams.TempFilePath)" -Level ERROR
                    Write-Log "Creating temp directory..." -Level INFO
                    try {
                        $newDir = New-Item -Path $MigrationParams.TempFilePath -ItemType Directory -Force
                        Write-Log "Created temp directory: $($newDir.FullName)" -Level INFO
                    } catch {
                        Write-Log "Failed to create temp directory: $($_.Exception.Message)" -Level ERROR
                        return $false
                    }
                } else {
                    Write-Log "Temp directory exists" -Level INFO
                    
                    # Check directory permissions
                    try {
                        $testFile = Join-Path $MigrationParams.TempFilePath "test_$(Get-Random).tmp"
                        [System.IO.File]::WriteAllText($testFile, "Test")
                        Write-Log "Successfully wrote test file to temp directory" -Level INFO
                        Remove-Item $testFile -Force
                    } catch {
                        Write-Log "Failed to write test file to temp directory: $($_.Exception.Message)" -Level ERROR
                        Write-Log "This may indicate a permissions issue" -Level ERROR
                    }
                }
                
                try {
                    Write-Log "Starting export for ${fullTableName} to $tempFilePath" -Level INFO
                    $exportResult = Export-TableData -Server $MigrationParams.SourceServer `
                                                    -Database $MigrationParams.SourceDB `
                                                    -Schema $schema `
                                                    -Table $tableName `
                                                    -OutputFile $tempFilePath `
                                                    -Format $MigrationParams.BCPFormat `
                                                    -Credential $MigrationParams.SourceCredential
                    
                    if (-not $exportResult) {
                        Write-Log "Failed to export data from ${fullTableName}" -Level ERROR
                        return $false
                    }
                    
                    # Verify temp file was created
                    if (-not (Test-Path $tempFilePath)) {
                        Write-Log "Temp file was not created: $tempFilePath" -Level ERROR
                        return $false
                    }
                    
                    $fileInfo = Get-Item $tempFilePath
                    $fileSizeBytes = $fileInfo.Length
                    $fileSizeMB = [Math]::Round($fileSizeBytes / 1MB, 2)
                    Write-Log "Temp file created: $tempFilePath, Size: $fileSizeBytes bytes ($fileSizeMB MB)" -Level INFO
                    
                    $importResult = Import-TableData -Server $MigrationParams.TargetServer `
                                                    -Database $MigrationParams.TargetDB `
                                                    -Schema $schema `
                                                    -Table $tableName `
                                                    -InputFile $tempFilePath `
                                                    -Format $MigrationParams.BCPFormat `
                                                    -BatchSize $MigrationParams.BatchSize `
                                                    -Credential $MigrationParams.TargetCredential
                    
                    if (-not $importResult) {
                        Write-Log "Failed to import data to ${fullTableName}" -Level ERROR
                        if (Test-Path $tempFilePath) {
                            Remove-Item $tempFilePath -Force -ErrorAction SilentlyContinue
                            Write-Log "Removed temporary file after error: $tempFilePath" -Level VERBOSE
                        }
                        return @{
                            Success = $false
                            TableName = $fullTableName
                            Error = "Failed to import data"
                            SourceRowCount = $sourceRowCount
                            TargetRowCount = 0
                            DataSizeBytes = $fileSizeBytes
                            DurationSeconds = 0
                        }
                    }
                    
                    # Return file size before deleting the temp file
                    $result = @{
                        Success = $true
                        TableName = $fullTableName
                        SourceRowCount = $sourceRowCount
                        TargetRowCount = $sourceRowCount
                        DataSizeBytes = $fileSizeBytes
                        DurationSeconds = 0
                    }
                    
                    if (Test-Path $tempFilePath) {
                        Remove-Item $tempFilePath -Force
                        Write-Log "Removed temporary file: $tempFilePath" -Level VERBOSE
                    }
                    
                    return $result
                } catch {
                    Write-Log ("Error migrating table ${fullTableName}`: {0}" -f $_.Exception.Message) -Level ERROR
                    
                    if (Test-Path $tempFilePath) {
                        Remove-Item $tempFilePath -Force -ErrorAction SilentlyContinue
                        Write-Log "Removed temporary file after error: $tempFilePath" -Level VERBOSE
                    }
                    
                    return $false
                }
            }
            
            Write-Log "Runspace initialized successfully" -Level INFO
        } catch {
            Write-Output "Error in runspace initialization: $($_.Exception.Message)"
            Write-Output "Stack trace: $($_.ScriptStackTrace)"
            throw
        }

        $schema = $Table.TABLE_SCHEMA
        $tableName = $Table.TABLE_NAME
        $fullTableName = "[$schema].[$tableName]"

        try {
            Write-Log "Starting migration of table $fullTableName..." -Level INFO

            # Get row count for source table
            function Get-TableRowCount {
                param (
                    [string]$Server,
                    [string]$Database,
                    [string]$Schema,
                    [string]$Table,
                    [System.Management.Automation.PSCredential]$Credential
                )

                $query = @"
                SELECT COUNT(*) AS RowCount FROM [$Schema].[$Table] WITH (NOLOCK);
"@

                try {
                    $bcpCommand = "sqlcmd -S `"$Server`" -d `"$Database`" -Q `"$query`" -h-1"
                    
                    if ($Credential) {
                        $bcpCommand += " -U `"$($Credential.UserName)`" -P `"$($Credential.GetNetworkCredential().Password)`""
                    } else {
                        $bcpCommand += " -E"
                    }
                    
                    $output = Invoke-Expression $bcpCommand -ErrorAction Stop
                    
                    # Parse the output to get the row count
                    $rowCount = 0
                    if ($output -match '^\s*(\d+)\s*$') {
                        $rowCount = [int]$matches[1]
                    }
                    
                    return $rowCount
                } catch {
                    Write-Log ("Error getting row count for table {0}: {1}" -f "[$Schema].[$Table]", $_.Exception.Message) -Level WARNING
                    return -1
                }
            }

            # Get row count for source table
            $sourceRowCount = Get-TableRowCount -Server $MigrationParams.SourceServer `
                                               -Database $MigrationParams.SourceDB `
                                               -Schema $schema `
                                               -Table $tableName `
                                               -Credential $MigrationParams.SourceCredential
            
            Write-Log "Source row count for ${fullTableName}: $sourceRowCount" -Level INFO
            
            # Record start time for statistics
            $startTime = Get-Date
            
            # Migrate table data
            $result = Migrate-TableData -Table $Table -MigrationParams $MigrationParams
            
            # Record end time for statistics
            $endTime = Get-Date
            $duration = $endTime - $startTime
            $durationSeconds = [Math]::Round($duration.TotalSeconds, 2)

            if ($result) {
                # Get target row count for validation
                $targetRowCount = Get-TableRowCount -Server $MigrationParams.TargetServer `
                                                   -Database $MigrationParams.TargetDB `
                                                   -Schema $schema `
                                                   -Table $tableName `
                                                   -Credential $MigrationParams.TargetCredential
                
                Write-Log "Target row count for ${fullTableName}: $targetRowCount" -Level INFO
                
                # Validate row counts
                if ($sourceRowCount -ne $targetRowCount) {
                    Write-Log "Row count mismatch for ${fullTableName}: Source=$sourceRowCount, Target=$targetRowCount" -Level WARNING
                } else {
                    Write-Log "Row count validation successful for ${fullTableName}: $sourceRowCount rows" -Level INFO
                }
                
                # Get file size if available
                $tempFilePath = Join-Path $MigrationParams.TempFilePath "$schema`_$tableName`_*.dat"
                $tempFiles = Get-ChildItem -Path $tempFilePath -ErrorAction SilentlyContinue
                $dataSizeBytes = 0
                
                if ($tempFiles -and $tempFiles.Count -gt 0) {
                    $dataSizeBytes = $tempFiles[0].Length
                }
                
                # Calculate statistics
                $rowsPerSecond = 0
                $mbPerSecond = 0
                
                if ($durationSeconds -gt 0 -and $sourceRowCount -gt 0) {
                    $rowsPerSecond = [Math]::Round($sourceRowCount / $durationSeconds, 2)
                    $mbPerSecond = [Math]::Round(($dataSizeBytes / 1MB) / $durationSeconds, 2)
                    
                    Write-Log "Migration statistics for ${fullTableName}:" -Level INFO
                    Write-Log "  Duration: $durationSeconds seconds" -Level INFO
                    Write-Log "  Rows/second: $rowsPerSecond" -Level INFO
                    Write-Log "  MB/second: $mbPerSecond" -Level INFO
                }
                
                Write-Log "Successfully migrated table $fullTableName" -Level INFO
                return @{ 
                    Success = $true
                    TableName = $fullTableName
                    SourceRowCount = $sourceRowCount
                    TargetRowCount = $targetRowCount
                    DataSizeBytes = $dataSizeBytes
                    DurationSeconds = $durationSeconds
                    RowsPerSecond = $rowsPerSecond
                    MBPerSecond = $mbPerSecond
                }
            }
            else {
                Write-Log "Failed to migrate table $fullTableName" -Level ERROR
                return @{ 
                    Success = $false
                    TableName = $fullTableName
                    SourceRowCount = $sourceRowCount
                    TargetRowCount = 0
                    DataSizeBytes = 0
                    DurationSeconds = $durationSeconds
                    RowsPerSecond = 0
                    MBPerSecond = 0
                }
            }
        }
        catch {
            $errorMessage = $_.Exception.Message
            Write-Log "Error migrating table $fullTableName`: $errorMessage" -Level ERROR
            return @{ 
                Success = $false
                TableName = $fullTableName
                Error = $errorMessage
                SourceRowCount = 0
                TargetRowCount = 0
                DataSizeBytes = 0
                DurationSeconds = 0
            }
        }
    }

    # Add module path to migration params
    $MigrationParams.ModulePath = Join-Path $PSScriptRoot ".."

    # Create and start runspaces for each table
    foreach ($table in $Tables) {
        $ps = [powershell]::Create()
        $ps.AddScript($scriptBlock).AddParameters(@{ Table = $table; MigrationParams = $MigrationParams })
        $ps.RunspacePool = $runspacePool
        $runspaceInfo = [PSCustomObject]@{
            Powershell = $ps
            Handle = $ps.BeginInvoke()
            Table = $table
            StartTime = Get-Date
        }
        [void]$runspaces.Add($runspaceInfo)

        $schema = $table.TABLE_SCHEMA
        $tableName = $table.TABLE_NAME
        Write-Log "Started migration of table [$schema].[$tableName]" -Level INFO
    }

    # Wait for all runspaces to complete and process results
    while ($runspaces.Count -gt 0) {
        # Check for completed runspaces
        $completedRunspaces = $runspaces | Where-Object { $_.Handle.IsCompleted }

        foreach ($runspace in $completedRunspaces) {
            try {
                $result = $runspace.Powershell.EndInvoke($runspace.Handle)

                # Add result to collection for summary
                $tableResults += $result

                if ($result.Success) {
                    $successfulTables++
                    Write-Log "Successfully migrated table $($result.TableName)" -Level INFO
                    
                    # Accumulate statistics
                    $totalSourceRows += $result.SourceRowCount
                    $totalTargetRows += $result.TargetRowCount
                    $totalDataSizeBytes += $result.DataSizeBytes
                    $totalDurationSeconds += $result.DurationSeconds
                }
                else {
                    $failedTables++
                    Write-Log "Failed to migrate table $($result.TableName)" -Level ERROR
                    if ($result.Error) {
                        Write-Log "  Error: $($result.Error)" -Level ERROR
                    }
                }

                # Calculate duration and log it
                $duration = (Get-Date) - $runspace.StartTime
                Write-Log "  Duration: $($duration.ToString('hh\:mm\:ss'))" -Level INFO
            }
            catch {
                $failedTables++
                Write-Log "Error processing runspace result: $($_.Exception.Message)" -Level ERROR
            }
            finally {
                # Dispose the runspace and remove from collection
                try {
                    $runspace.Powershell.Dispose()
                }
                catch {
                    Write-Log "Error disposing runspace: $($_.Exception.Message)" -Level WARNING
                }
                [void]$runspaces.Remove($runspace)
            }
        }

        if ($runspaces.Count -gt 0) {
            Start-Sleep -Milliseconds 500
        }
    }

    # Close the runspace pool
    try {
        $runspacePool.Close()
        $runspacePool.Dispose()
    }
    catch {
        Write-Log "Error closing runspace pool: $($_.Exception.Message)" -Level WARNING
    }

    # Calculate overall statistics
    $endTime = Get-Date
    $totalDuration = $endTime - $startTime
    $totalDurationFormatted = $totalDuration.ToString('hh\:mm\:ss')
    
    # Calculate average transfer rates
    $avgRowsPerSecond = 0
    $avgMBPerSecond = 0
    
    if ($totalDurationSeconds -gt 0) {
        $avgRowsPerSecond = [Math]::Round($totalSourceRows / $totalDuration.TotalSeconds, 2)
        $avgMBPerSecond = [Math]::Round(($totalDataSizeBytes / 1MB) / $totalDuration.TotalSeconds, 2)
    }
    
    # Generate migration summary
    $totalDataSizeMB = [Math]::Round($totalDataSizeBytes / 1MB, 2)
    
    $summary = @"
    
========== MIGRATION SUMMARY ==========
Total tables: $($Tables.Count)
Successfully migrated: $successfulTables
Failed: $failedTables
Total rows migrated: $totalSourceRows
Total data size: $totalDataSizeMB MB
Total duration: $totalDurationFormatted
Average transfer rate: $avgRowsPerSecond rows/sec, $avgMBPerSecond MB/sec
======================================
"@
    
    Write-Log $summary -Level INFO
    
    # Return detailed results
    return @{
        TotalTables = $Tables.Count
        SuccessfulTables = $successfulTables
        FailedTables = $failedTables
        TotalSourceRows = $totalSourceRows
        TotalTargetRows = $totalTargetRows
        TotalDataSizeBytes = $totalDataSizeBytes
        TotalDurationSeconds = $totalDuration.TotalSeconds
        StartTime = $startTime
        EndTime = $endTime
        AvgRowsPerSecond = $avgRowsPerSecond
        AvgMBPerSecond = $avgMBPerSecond
        TableResults = $tableResults
        Summary = $summary
    }
}

Export-ModuleMember -Function Start-ParallelTableMigration
