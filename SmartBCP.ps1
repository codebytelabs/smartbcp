#!/usr/bin/env pwsh
#
# SmartBCP.ps1
# A high-performance bulk data transfer utility for SQL Server using native tools
#

[CmdletBinding()]
param (
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$ConfigFile,
    
    [Parameter(Mandatory = $false)]
    [string]$LogFile,
    
    [Parameter(Mandatory = $false)]
    [switch]$DetailedLogging
)

# ULTRA SIMPLE PATH APPROACH WITH CODE EVALUATION
# =================================================
# Instead of dot-sourcing (which may open the file in an editor), we load the module code using Get-Content and Invoke-Expression.
# This ensures the module's functions are defined in the current session.

# Get script location using the absolute path
$scriptPath = $MyInvocation.MyCommand.Path
$scriptDir = Split-Path -Parent $scriptPath

# Echo the directory for debugging
Write-Host "Script directory: $scriptDir"

# List of module filenames to load
$moduleFiles = @("Configuration-Enhanced.psm1", "Constraints.psm1", "TableInfo.psm1", "DataMovement.psm1", "Logging.psm1")
foreach ($file in $moduleFiles) {
    # Use [IO.Path]::Combine to build the module file path robustly
    $modPath = [System.IO.Path]::Combine($scriptDir, "Modules", $file)
    if (-not (Test-Path -Path $modPath)) {
        Write-Error "Module file not found: $modPath"
        exit 1
    }
    $modContent = Get-Content -Path $modPath -Raw
    Invoke-Expression $modContent
}

# Main function
function Start-SmartBcp {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [string]$ConfigFile,
        
        [Parameter(Mandatory = $false)]
        [string]$LogFile
    )
    
    $startTime = Get-Date
    Write-SmartBcpLog -Message "Starting Smart BCP operation" -Level "INFO" -LogFile $LogFile
    
    try {
        # Log current paths for debugging
        Write-SmartBcpLog -Message "Script directory: $scriptDir" -Level "INFO" -LogFile $LogFile
        
        # Load configuration
        Write-SmartBcpLog -Message "Loading configuration from $ConfigFile" -Level "INFO" -LogFile $LogFile
        $config = Import-SmartBcpConfig -ConfigFile $ConfigFile
        
        $sourceServer = $config.source.server
        $sourceDB = $config.source.database
        $sourceAuth = $config.source.authentication
        $sourceUser = if ($sourceAuth -eq "sql") { $config.source.username } else { "" }
        $sourcePass = if ($sourceAuth -eq "sql") { $config.source.password } else { "" }
        
        $destServer = $config.destination.server
        $destDB = $config.destination.database
        $destAuth = $config.destination.authentication
        $destUser = if ($destAuth -eq "sql") { $config.destination.username } else { "" }
        $destPass = if ($destAuth -eq "sql") { $config.destination.password } else { "" }
        
        $maxThreads = $config.options.maxThreads
        $batchSize = $config.options.batchSize
        $tempFolder = $config.options.tempFolder
        $truncateTarget = $config.options.truncateTarget
        
        Write-SmartBcpLog -Message "Source: $sourceServer.$sourceDB" -Level "INFO" -LogFile $LogFile
        Write-SmartBcpLog -Message "Destination: $destServer.$destDB" -Level "INFO" -LogFile $LogFile
        
        # Create temp folder if it doesn't exist
        if (-not (Test-Path -Path $tempFolder)) {
            New-Item -Path $tempFolder -ItemType Directory -Force | Out-Null
            Write-SmartBcpLog -Message "Created temporary folder: $tempFolder" -Level "INFO" -LogFile $LogFile
        }
        
        # Expand table wildcards to specific tables
        Write-SmartBcpLog -Message "Expanding table patterns" -Level "INFO" -LogFile $LogFile
        $tables = Expand-TableWildcards -Server $sourceServer -Database $sourceDB -TablePatterns $config.tables -Authentication $sourceAuth -Username $sourceUser -Password $sourcePass
        Write-SmartBcpLog -Message "Found $($tables.Count) tables to process" -Level "INFO" -LogFile $LogFile
        
        # Get all constraints and dependencies
        Write-SmartBcpLog -Message "Getting foreign key constraints" -Level "INFO" -LogFile $LogFile
        $constraints = Get-ForeignKeyConstraints -Server $destServer -Database $destDB -Authentication $destAuth -Username $destUser -Password $destPass
        Write-SmartBcpLog -Message "Found $($constraints.Count) foreign key constraints" -Level "INFO" -LogFile $LogFile
        
        Write-SmartBcpLog -Message "Getting table dependencies" -Level "INFO" -LogFile $LogFile
        $dependencies = Get-TableDependencies -Server $sourceServer -Database $sourceDB -Authentication $sourceAuth -Username $sourceUser -Password $sourcePass
        
        # Determine table processing order
        Write-SmartBcpLog -Message "Determining table processing order" -Level "INFO" -LogFile $LogFile
        try {
            $orderedTables = Get-TableProcessingOrder -Tables $tables -Dependencies $dependencies
        }
        catch {
            Write-SmartBcpLog -Message "Error determining table order: $_" -Level "ERROR" -LogFile $LogFile
            throw $_
        }
        
        # Drop all foreign key constraints
        Write-SmartBcpLog -Message "Dropping foreign key constraints" -Level "INFO" -LogFile $LogFile
        foreach ($constraint in $constraints) {
            try {
                $sqlParams = @{
                    ServerInstance = $destServer
                    Database = $destDB
                    Query = $constraint.DropScript
                }
                if ($destAuth -eq "sql") {
                    $sqlParams.Add("Username", $destUser)
                    $sqlParams.Add("Password", $destPass)
                }
                Invoke-Sqlcmd @sqlParams
                Write-SmartBcpLog -Message "Dropped constraint: $($constraint.FKName)" -Level "INFO" -LogFile $LogFile
            }
            catch {
                $errorMessage = "Error dropping constraint {0}: {1}" -f $constraint.FKName, $_.Exception.Message
                Write-SmartBcpLog -Message $errorMessage -Level "WARNING" -LogFile $LogFile
            }
        }
        
        # Process tables in order
        Write-SmartBcpLog -Message "Processing tables in dependency order" -Level "INFO" -LogFile $LogFile
        $jobQueue = @()
        $runningJobs = @()
        
        foreach ($table in $orderedTables) {
            Write-SmartBcpLog -Message "Processing table: $table" -Level "INFO" -LogFile $LogFile
            
            $partitionInfo = Get-TablePartitions -Server $sourceServer -Database $sourceDB -TableName $table -Authentication $sourceAuth -Username $sourceUser -Password $sourcePass
            
            if ($truncateTarget) {
                try {
                    $sqlParams = @{
                        ServerInstance = $destServer
                        Database = $destDB
                        Query = "TRUNCATE TABLE [$table]"
                    }
                    if ($destAuth -eq "sql") {
                        $sqlParams.Add("Username", $destUser)
                        $sqlParams.Add("Password", $destPass)
                    }
                    Invoke-Sqlcmd @sqlParams
                    Write-SmartBcpLog -Message "Truncated table: $table" -Level "INFO" -LogFile $LogFile
                }
                catch {
                    Write-SmartBcpLog -Message "Could not truncate $table. Attempting DELETE instead." -Level "WARNING" -LogFile $LogFile
                    try {
                        $sqlParams = @{
                            ServerInstance = $destServer
                            Database = $destDB
                            Query = "DELETE FROM [$table]"
                        }
                        if ($destAuth -eq "sql") {
                            $sqlParams.Add("Username", $destUser)
                            $sqlParams.Add("Password", $destPass)
                        }
                        Invoke-Sqlcmd @sqlParams
                        Write-SmartBcpLog -Message "Deleted all rows from table: $table" -Level "INFO" -LogFile $LogFile
                    }
                    catch {
                        $errorMessage = "Error clearing table {0}: {1}" -f $table, $_.Exception.Message
                        Write-SmartBcpLog -Message $errorMessage -Level "ERROR" -LogFile $LogFile
                        throw $_
                    }
                }
            }
            
            foreach ($partition in $partitionInfo.Partitions) {
                $partitionLabel = if ($partitionInfo.IsPartitioned) { "partition $partition" } else { "single partition" }
                Write-SmartBcpLog -Message "Preparing to process $table ($partitionLabel)" -Level "INFO" -LogFile $LogFile
                
                # Read the code of the necessary module files
                $dataMovementCode = Get-Content -Path "$scriptDir\Modules\DataMovement.psm1" -Raw
                $loggingCode = Get-Content -Path "$scriptDir\Modules\Logging.psm1" -Raw
                
                $jobParams = @{
                    ScriptBlock = {
                        param($srcServer, $srcDB, $table, $isPartitioned, $partitionFunc, $partitionCol, $partitionNum, 
                              $dstServer, $dstDB, $tmpFolder, $batchSz, $logFile, 
                              $srcAuth, $srcUser, $srcPass, $dstAuth, $dstUser, $dstPass,
                              $dataMovementCode, $loggingCode)
                        try {
                            # Evaluate the module code directly
                            Invoke-Expression $loggingCode
                            Invoke-Expression $dataMovementCode
                            
                            $partitionLabel = if ($isPartitioned) { "partition $partitionNum" } else { "single partition" }
                            Write-SmartBcpLog -Message "Starting export of $table ($partitionLabel)" -Level "INFO" -LogFile $logFile
                            
                            $outputFile = Export-TablePartition -SourceServer $srcServer -SourceDatabase $srcDB `
                                          -TableName $table -IsPartitioned $isPartitioned -PartitionFunction $partitionFunc `
                                          -PartitionColumn $partitionCol -PartitionNumber $partitionNum -OutputPath $tmpFolder `
                                          -Authentication $srcAuth -Username $srcUser -Password $srcPass
                            
                            Write-SmartBcpLog -Message "Exported $table ($partitionLabel) to $outputFile" -Level "SUCCESS" -LogFile $logFile
                            
                            Write-SmartBcpLog -Message "Starting import of $table ($partitionLabel)" -Level "INFO" -LogFile $logFile
                            Import-TablePartition -DestServer $dstServer -DestDatabase $dstDB `
                                             -TableName $table -InputFile $outputFile -BatchSize $batchSz `
                                             -Authentication $dstAuth -Username $dstUser -Password $dstPass
                            
                            Write-SmartBcpLog -Message "Successfully imported $table ($partitionLabel)" -Level "SUCCESS" -LogFile $logFile
                            return $true
                        }
                        catch {
                            $errorMessage = "Error processing {0} ({1}): {2}" -f $table, $partitionLabel, $_.Exception.Message
                            Write-Error $errorMessage
                            return $false
                        }
                    }
                    ArgumentList = @(
                        $sourceServer, $sourceDB, $table, $partitionInfo.IsPartitioned, 
                        $partitionInfo.PartitionFunction, $partitionInfo.PartitionColumn, $partition, 
                        $destServer, $destDB, $tempFolder, $batchSize, $LogFile,
                        $sourceAuth, $sourceUser, $sourcePass, $destAuth, $destUser, $destPass,
                        $dataMovementCode, $loggingCode
                    )
                }
                $jobQueue += $jobParams
            }
        }
        
        $totalJobs = $jobQueue.Count
        $completedJobs = 0
        $failedJobs = 0
        
        Write-SmartBcpLog -Message "Starting parallel processing with $maxThreads threads for $totalJobs total jobs" -Level "INFO" -LogFile $LogFile
        
        while ($jobQueue.Count -gt 0 -or $runningJobs.Count -gt 0) {
            while ($jobQueue.Count -gt 0 -and $runningJobs.Count -lt $maxThreads) {
                $jobParams = $jobQueue[0]
                $jobQueue = $jobQueue[1..($jobQueue.Count-1)]
                $job = Start-Job @jobParams
                $runningJobs += $job
                Write-SmartBcpLog -Message "Started job $($job.Id) - $($runningJobs.Count)/$maxThreads threads active" -Level "INFO" -LogFile $LogFile
            }
            
            $stillRunning = @()
            foreach ($job in $runningJobs) {
                if ($job.State -eq "Completed") {
                    $result = Receive-Job -Job $job -ErrorAction SilentlyContinue
                    if ($result -eq $true) {
                        $completedJobs++
                        Write-SmartBcpLog -Message "Job $($job.Id) completed successfully ($completedJobs/$totalJobs)" -Level "SUCCESS" -LogFile $LogFile
                    } else {
                        $failedJobs++
                        Write-SmartBcpLog -Message "Job $($job.Id) failed ($failedJobs/$totalJobs)" -Level "ERROR" -LogFile $LogFile
                    }
                    Remove-Job -Job $job
                }
                elseif ($job.State -eq "Failed") {
                    $failedJobs++
                    $errorDetails = Receive-Job -Job $job -ErrorAction SilentlyContinue
                    Write-SmartBcpLog -Message "Job $($job.Id) failed with unhandled error: $($job.ChildJobs[0].JobStateInfo.Reason) $errorDetails" -Level "ERROR" -LogFile $LogFile
                    Remove-Job -Job $job
                }
                else {
                    $stillRunning += $job
                }
            }
            $runningJobs = $stillRunning
            if ($runningJobs.Count -ge $maxThreads -or ($jobQueue.Count -eq 0 -and $runningJobs.Count -gt 0)) {
                Start-Sleep -Seconds 2
            }
        }
        
        Write-SmartBcpLog -Message "Recreating foreign key constraints" -Level "INFO" -LogFile $LogFile
        foreach ($constraint in $constraints) {
            try {
                $sqlParams = @{
                    ServerInstance = $destServer
                    Database = $destDB
                    Query = $constraint.CreateScript
                }
                if ($destAuth -eq "sql") {
                    $sqlParams.Add("Username", $destUser)
                    $sqlParams.Add("Password", $destPass)
                }
                Invoke-Sqlcmd @sqlParams
                Write-SmartBcpLog -Message "Recreated constraint: $($constraint.FKName)" -Level "INFO" -LogFile $LogFile
            }
            catch {
                $errorMessage = "Failed to recreate constraint {0}: {1}" -f $constraint.FKName, $_.Exception.Message
                Write-SmartBcpLog -Message $errorMessage -Level "ERROR" -LogFile $LogFile
            }
        }
        
        $endTime = Get-Date
        $duration = $endTime - $startTime
        Write-SmartBcpLog -Message "Smart BCP operation completed in $($duration.TotalMinutes.ToString('0.00')) minutes" -Level "SUCCESS" -LogFile $LogFile
        Write-SmartBcpLog -Message "Successfully processed: $completedJobs jobs" -Level "SUCCESS" -LogFile $LogFile
        if ($failedJobs -gt 0) {
            Write-SmartBcpLog -Message "Failed jobs: $failedJobs" -Level "ERROR" -LogFile $LogFile
        }
    }
    catch {
        $errorMessage = "Error in Smart BCP operation: {0}" -f $_.Exception.Message
        Write-SmartBcpLog -Message $errorMessage -Level "ERROR" -LogFile $LogFile
        throw $_
    }
    finally {
        if ($tempFolder -and (Test-Path -Path $tempFolder)) {
            $tempFiles = Get-ChildItem -Path $tempFolder -Filter "*.dat" -ErrorAction SilentlyContinue
            if ($tempFiles -and $tempFiles.Count -gt 0) {
                Write-SmartBcpLog -Message "Cleaning up $($tempFiles.Count) temporary files" -Level "INFO" -LogFile $LogFile
                Remove-Item -Path "$tempFolder\*.dat" -Force -ErrorAction SilentlyContinue
            }
        }
    }
}

if ($DetailedLogging) {
    $VerbosePreference = "Continue"
}

try {
    Start-SmartBcp -ConfigFile $ConfigFile -LogFile $LogFile
    exit 0
}
catch {
    $errorMessage = "SmartBCP failed: {0}" -f $_.Exception.Message
    Write-Error $errorMessage
    exit 1
}
