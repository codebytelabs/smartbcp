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

# Import modules
$modulePath = Join-Path -Path $PSScriptRoot -ChildPath "Modules"
Import-Module -Name (Join-Path -Path $modulePath -ChildPath "Configuration-Enhanced.psm1") -Force
Import-Module -Name (Join-Path -Path $modulePath -ChildPath "Constraints.psm1") -Force
Import-Module -Name (Join-Path -Path $modulePath -ChildPath "TableInfo.psm1") -Force
Import-Module -Name (Join-Path -Path $modulePath -ChildPath "DataMovement.psm1") -Force
Import-Module -Name (Join-Path -Path $modulePath -ChildPath "Logging.psm1") -Force

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
            # This catch block should no longer be needed since we handle circular dependencies in the function,
            # but we'll keep it as a fallback just in case
            Write-SmartBcpLog -Message "Error determining table order: $_" -Level "ERROR" -LogFile $LogFile
            throw $_
        }
        
        # Drop all foreign key constraints
        Write-SmartBcpLog -Message "Dropping foreign key constraints" -Level "INFO" -LogFile $LogFile
        foreach ($constraint in $constraints) {
            try {
                # Set SQL authentication parameters if needed
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
            
            # Get partition information
            $partitionInfo = Get-TablePartitions -Server $sourceServer -Database $sourceDB -TableName $table -Authentication $sourceAuth -Username $sourceUser -Password $sourcePass
            
            # Truncate target table if specified
            if ($truncateTarget) {
                try {
                    # Set SQL authentication parameters if needed
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
                        # Set SQL authentication parameters if needed
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
            
            # Process each partition
            foreach ($partition in $partitionInfo.Partitions) {
                $partitionLabel = if ($partitionInfo.IsPartitioned) { "partition $partition" } else { "single partition" }
                Write-SmartBcpLog -Message "Preparing to process $table ($partitionLabel)" -Level "INFO" -LogFile $LogFile
                
                $jobParams = @{
                    ScriptBlock = {
                        param($srcServer, $srcDB, $table, $isPartitioned, $partitionFunc, $partitionCol, $partitionNum, 
                              $dstServer, $dstDB, $tmpFolder, $batchSz, $logFile, 
                              $srcAuth, $srcUser, $srcPass, $dstAuth, $dstUser, $dstPass)
                        
                        try {
                            # Import needed modules
                            Import-Module -Name (Join-Path -Path $using:modulePath -ChildPath "DataMovement.psm1") -Force
                            Import-Module -Name (Join-Path -Path $using:modulePath -ChildPath "Logging.psm1") -Force
                            
                            $partitionLabel = if ($isPartitioned) { "partition $partitionNum" } else { "single partition" }
                            Write-SmartBcpLog -Message "Starting export of $table ($partitionLabel)" -Level "INFO" -LogFile $logFile
                            
                            # Export
                            $outputFile = Export-TablePartition -SourceServer $srcServer -SourceDatabase $srcDB `
                                          -TableName $table -IsPartitioned $isPartitioned -PartitionFunction $partitionFunc `
                                          -PartitionColumn $partitionCol -PartitionNumber $partitionNum -OutputPath $tmpFolder `
                                          -Authentication $srcAuth -Username $srcUser -Password $srcPass
                            
                            Write-SmartBcpLog -Message "Exported $table ($partitionLabel) to $outputFile" -Level "SUCCESS" -LogFile $logFile
                            
                            # Import
                            Write-SmartBcpLog -Message "Starting import of $table ($partitionLabel)" -Level "INFO" -LogFile $logFile
                            Import-TablePartition -DestServer $dstServer -DestDatabase $dstDB `
                                             -TableName $table -InputFile $outputFile -BatchSize $batchSz `
                                             -Authentication $dstAuth -Username $dstUser -Password $dstPass
                            
                            Write-SmartBcpLog -Message "Successfully imported $table ($partitionLabel)" -Level "SUCCESS" -LogFile $logFile
                            return $true
                        }
                        catch {
                            $errorMessage = "Error processing {0} ({1}): {2}" -f $table, $partitionLabel, $_.Exception.Message
                            Write-SmartBcpLog -Message $errorMessage -Level "ERROR" -LogFile $logFile
                            return $false
                        }
                    }
                    ArgumentList = @(
                        $sourceServer, $sourceDB, $table, $partitionInfo.IsPartitioned, 
                        $partitionInfo.PartitionFunction, $partitionInfo.PartitionColumn, $partition, 
                        $destServer, $destDB, $tempFolder, $batchSize, $LogFile,
                        $sourceAuth, $sourceUser, $sourcePass, $destAuth, $destUser, $destPass
                    )
                }
                
                $jobQueue += $jobParams
            }
        }
        
        # Process job queue with throttling
        $totalJobs = $jobQueue.Count
        $completedJobs = 0
        $failedJobs = 0
        
        Write-SmartBcpLog -Message "Starting parallel processing with $maxThreads threads for $totalJobs total jobs" -Level "INFO" -LogFile $LogFile
        
        while ($jobQueue.Count -gt 0 -or $runningJobs.Count -gt 0) {
            # Start new jobs if slots available
            while ($jobQueue.Count -gt 0 -and $runningJobs.Count -lt $maxThreads) {
                $jobParams = $jobQueue[0]
                $jobQueue = $jobQueue[1..($jobQueue.Count-1)]
                
                $job = Start-Job @jobParams
                $runningJobs += $job
                Write-SmartBcpLog -Message "Started job $($job.Id) - $($runningJobs.Count)/$maxThreads threads active" -Level "INFO" -LogFile $LogFile
            }
            
            # Check for completed jobs
            $stillRunning = @()
            foreach ($job in $runningJobs) {
                if ($job.State -eq "Completed") {
                    $result = Receive-Job -Job $job
                    
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
                    Write-SmartBcpLog -Message "Job $($job.Id) failed with unhandled error: $($job.ChildJobs[0].JobStateInfo.Reason)" -Level "ERROR" -LogFile $LogFile
                    Remove-Job -Job $job
                }
                else {
                    $stillRunning += $job
                }
            }
            $runningJobs = $stillRunning
            
            # Pause before checking again
            if ($runningJobs.Count -ge $maxThreads -or ($jobQueue.Count -eq 0 -and $runningJobs.Count -gt 0)) {
                Start-Sleep -Seconds 2
            }
        }
        
        # Recreate all foreign key constraints
        Write-SmartBcpLog -Message "Recreating foreign key constraints" -Level "INFO" -LogFile $LogFile
        foreach ($constraint in $constraints) {
            try {
                # Set SQL authentication parameters if needed
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
            } catch {
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
        # Clean up any leftover temp files
        if (Test-Path -Path $tempFolder) {
            $tempFiles = Get-ChildItem -Path $tempFolder -Filter "*.dat"
            if ($tempFiles.Count -gt 0) {
                Write-SmartBcpLog -Message "Cleaning up $($tempFiles.Count) temporary files" -Level "INFO" -LogFile $LogFile
                Remove-Item -Path "$tempFolder\*.dat" -Force
            }
        }
    }
}

# Execute main function
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
