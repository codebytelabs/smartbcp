# Core Module for SmartBCP

function Start-SmartBCP {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$SourceServer,
        
        [Parameter(Mandatory=$true)]
        [string]$SourceDB,
        
        [Parameter(Mandatory=$true)]
        [string]$TargetServer,
        
        [Parameter(Mandatory=$true)]
        [string]$TargetDB,
        
        [Parameter(Mandatory=$false)]
        [string[]]$IncludeSchemas,
        
        [Parameter(Mandatory=$false)]
        [string[]]$ExcludeSchemas,
        
        [Parameter(Mandatory=$false)]
        [string[]]$IncludeTables,
        
        [Parameter(Mandatory=$false)]
        [string[]]$ExcludeTables,
        
        [Parameter(Mandatory=$false)]
        [switch]$TruncateTargetTables,
        
        [Parameter(Mandatory=$false)]
        [switch]$ManageForeignKeys,
        
        [Parameter(Mandatory=$false)]
        [int]$ParallelTasks = 4,
        
        [Parameter(Mandatory=$false)]
        [ValidateSet("native", "char", "widechar")]
        [string]$BCPFormat = "native",

        [Parameter(Mandatory=$false)]
        [string]$TempPath = ".\Temp",
        
        [Parameter(Mandatory=$false)]
        [System.Management.Automation.PSCredential]$SourceCredential,
        
        [Parameter(Mandatory=$false)]
        [System.Management.Automation.PSCredential]$TargetCredential
    )

    $runspacePool = $null
    
    try {
        # Initialize logging
        $logDir = Join-Path $PSScriptRoot "..\Logs"
        Initialize-Logging -LogDir $logDir
        
        Write-Log "Starting SmartBCP utility..." -Level INFO
        
        # Validate parameters
        Validate-Parameters -SourceServer $SourceServer -SourceDB $SourceDB -TargetServer $TargetServer -TargetDB $TargetDB
        
        # Create temp directory for BCP files
        $tempDir = Join-Path (Resolve-Path (Join-Path $PSScriptRoot "..")) "Temp"
        Write-Log "Temp directory path: $tempDir" -Level INFO
        
        if (-not (Test-Path $tempDir)) {
            Write-Log "Creating temp directory: $tempDir" -Level INFO
            New-Item -Path $tempDir -ItemType Directory -Force | Out-Null
            
            # Verify the directory was created
            if (Test-Path $tempDir) {
                Write-Log "Temp directory created successfully" -Level INFO
            } else {
                Write-Log "Failed to create temp directory" -Level ERROR
                throw "Failed to create temp directory: $tempDir"
            }
        } else {
            Write-Log "Temp directory already exists" -Level INFO
        }
        
        # Test source database connection
        $sourceCredParam = @{}
        if ($SourceCredential) {
            $sourceCredParam.Add("Credential", $SourceCredential)
        }

        $sourceConnection = Test-DatabaseConnection -Server $SourceServer -Database $SourceDB @sourceCredParam
        
        # Test target database connection
        $targetCredParam = @{}
        if ($TargetCredential) {
            $targetCredParam.Add("Credential", $TargetCredential)
        }

        $targetConnection = Test-DatabaseConnection -Server $TargetServer -Database $TargetDB @targetCredParam
        
        # Test BCP command availability
        Test-BCPAvailability

        # Get tables from source database
        $tables = Get-Tables -Server $SourceServer -Database $SourceDB @sourceCredParam `
                           -SchemaFilter $(if ($IncludeSchemas) { $IncludeSchemas -join ',' } else { '%' }) `
                           -TableFilter $(if ($IncludeTables) { $IncludeTables -join ',' } else { '%' })
        
        # Filter out excluded schemas and tables
        if ($ExcludeSchemas) {
            $tables = $tables | Where-Object { $ExcludeSchemas -notcontains $_.TABLE_SCHEMA }
        }
        
        if ($ExcludeTables) {
            $tables = $tables | Where-Object { $ExcludeTables -notcontains $_.TABLE_NAME }
        }
        
        # Initialize migration parameters
        $migrationParams = @{
            SourceServer = $SourceServer
            SourceDB = $SourceDB
            TargetServer = $TargetServer
            TargetDB = $TargetDB
            TempFilePath = $tempDir
            BCPFormat = $BCPFormat
            SourceCredential = $SourceCredential
            TargetCredential = $TargetCredential
        }
        
        # Start parallel table migration
        $runspacePool = [runspacefactory]::CreateRunspacePool(1, $ParallelTasks)
        $runspacePool.Open()
        $migrationResults = Start-ParallelTableMigration -Tables $tables -ParallelTasks $ParallelTasks -MigrationParams $migrationParams
        
        # Log the migration summary
        Write-Log "Migration completed." -Level INFO
        Write-Log $migrationResults.Summary -Level INFO
        
        # Validate row counts if requested
        if ($migrationResults.TotalSourceRows -gt 0 -and $migrationResults.TotalTargetRows -gt 0) {
            if ($migrationResults.TotalSourceRows -eq $migrationResults.TotalTargetRows) {
                Write-Log "Row count validation successful: Source=$($migrationResults.TotalSourceRows), Target=$($migrationResults.TotalTargetRows)" -Level INFO
            } else {
                Write-Log "Row count validation failed: Source=$($migrationResults.TotalSourceRows), Target=$($migrationResults.TotalTargetRows)" -Level WARNING
                Write-Log "Row count difference: $($migrationResults.TotalSourceRows - $migrationResults.TotalTargetRows)" -Level WARNING
            }
        }
        
        # Generate detailed table report
        if ($migrationResults.TableResults.Count -gt 0) {
            Write-Log "Detailed Table Migration Report:" -Level INFO
            Write-Log "----------------------------------------" -Level INFO
            
            foreach ($tableResult in $migrationResults.TableResults | Where-Object { $_.Success -eq $true } | Sort-Object -Property DurationSeconds -Descending) {
                $durationFormatted = [TimeSpan]::FromSeconds($tableResult.DurationSeconds).ToString("hh\:mm\:ss")
                $dataSizeMB = [Math]::Round($tableResult.DataSizeBytes / 1MB, 2)
                
                Write-Log "Table: $($tableResult.TableName)" -Level INFO
                Write-Log "  Status: Success" -Level INFO
                Write-Log "  Rows: $($tableResult.SourceRowCount)" -Level INFO
                Write-Log "  Data Size: $dataSizeMB MB" -Level INFO
                Write-Log "  Duration: $durationFormatted" -Level INFO
                if ($tableResult.RowsPerSecond -gt 0) {
                    Write-Log "  Transfer Rate: $($tableResult.RowsPerSecond) rows/sec, $($tableResult.MBPerSecond) MB/sec" -Level INFO
                }
                Write-Log "----------------------------------------" -Level INFO
            }
            
            if ($migrationResults.FailedTables -gt 0) {
                Write-Log "Failed Tables:" -Level ERROR
                Write-Log "----------------------------------------" -Level ERROR
                
                foreach ($tableResult in $migrationResults.TableResults | Where-Object { $_.Success -eq $false }) {
                    Write-Log "Table: $($tableResult.TableName)" -Level ERROR
                    Write-Log "  Error: $($tableResult.Error)" -Level ERROR
                    Write-Log "----------------------------------------" -Level ERROR
                }
            }
        }
        
        return $migrationResults
    }
    catch {
        Write-Log "Critical error in SmartBCP: $($_.Exception.Message)" -Level ERROR
        throw $_
    }
    finally {
        if ($null -ne $runspacePool) {
            try {
                # Try to close the runspace pool if it's not already closed
                $runspacePool.Close()
            }
            catch {
                Write-Log "Error closing runspacepool" -Level Warning
            }
            
            try {
                $runspacePool.Dispose()
            }
            catch {
                Write-Log "Error disposing runspacepool" -Level Warning
            }
        }
    }
}

# Export module members
Export-ModuleMember -Function Start-SmartBCP
