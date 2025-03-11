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

    # Initialize counters
    $successfulTables = 0
    $failedTables = 0

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
                    Write-Log "Temp file created: $tempFilePath, Size: $($fileInfo.Length) bytes" -Level INFO
                    
                    $importResult = Import-TableData -Server $MigrationParams.TargetServer `
                                                    -Database $MigrationParams.TargetDB `
                                                    -Schema $schema `
                                                    -Table $tableName `
                                                    -InputFile $tempFilePath `
                                                    -Format $MigrationParams.BCPFormat `
                                                    -Credential $MigrationParams.TargetCredential
                    
                    if (-not $importResult) {
                        Write-Log "Failed to import data to ${fullTableName}" -Level ERROR
                        return $false
                    }
                    
                    if (Test-Path $tempFilePath) {
                        Remove-Item $tempFilePath -Force
                        Write-Log "Removed temporary file: $tempFilePath" -Level VERBOSE
                    }
                    
                    return $true
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

            # Migrate table data
            $result = Migrate-TableData -Table $Table -MigrationParams $MigrationParams

            if ($result) {
                Write-Log "Successfully migrated table $fullTableName" -Level INFO
                return @{ Success = $true; TableName = $fullTableName }
            }
            else {
                Write-Log "Failed to migrate table $fullTableName" -Level ERROR
                return @{ Success = $false; TableName = $fullTableName }
            }
        }
        catch {
            $errorMessage = $_.Exception.Message
            Write-Log "Error migrating table $fullTableName`: $errorMessage" -Level ERROR
            return @{ Success = $false; TableName = $fullTableName; Error = $errorMessage }
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

                if ($result.Success) {
                    $successfulTables++
                    Write-Log "Successfully migrated table $($result.TableName)" -Level INFO
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

    return @{
        TotalTables = $Tables.Count
        SuccessfulTables = $successfulTables
        FailedTables = $failedTables
    }
}

Export-ModuleMember -Function Start-ParallelTableMigration
