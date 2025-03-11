function Test-BCPAvailability {
    try {
        $bcpVersion = Invoke-Expression "bcp -v" -ErrorAction Stop
        Write-Log "BCP command is available: $bcpVersion" -Level INFO
        return $true
    } catch {
        Write-Log "BCP command is not available. Please ensure SQL Server tools are installed and in the PATH." -Level ERROR
        throw "BCP command is not available: $($_.Exception.Message)"
    }
}

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
    Write-Log "  Credential: $(if ($Credential) { "Provided" } else { "Not provided" })" -Level INFO
    
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
            Write-Log "Exception details: $($_.Exception | Format-List -Force | Out-String)" -Level ERROR
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
        
        # Use Start-Process instead of Invoke-Expression for better error handling
        $processStartInfo = New-Object System.Diagnostics.ProcessStartInfo
        $processStartInfo.FileName = "bcp"
        
        # Build arguments array
        $argList = @(
            "`"$fullTableName`"",
            "out",
            "`"$OutputFile`"",
            $formatOptions,
            "-S", "`"$Server`"",
            "-d", "`"$Database`""
        )
        
        if ($Credential) {
            $argList += "-U"
            $argList += "`"$($Credential.UserName)`""
            $argList += "-P"
            $argList += "`"$($Credential.GetNetworkCredential().Password)`""
        } else {
            $argList += "-T"
        }
        
        $argList += "-t"
        
        $processStartInfo.Arguments = $argList -join " "
        $processStartInfo.UseShellExecute = $false
        $processStartInfo.RedirectStandardOutput = $true
        $processStartInfo.RedirectStandardError = $true
        $processStartInfo.CreateNoWindow = $true
        
        $process = New-Object System.Diagnostics.Process
        $process.StartInfo = $processStartInfo
        
        # Log the BCP command (without password)
        $logCommand = $processStartInfo.Arguments
        if ($Credential) {
            $logCommand = $logCommand -replace " -P `"[^`"]+`"", " -P `"********`""
        }
        Write-Log "BCP command: bcp $logCommand" -Level INFO
        
        # Start the process
        $process.Start() | Out-Null
        
        # Capture output
        $output = $process.StandardOutput.ReadToEnd()
        $errorOutput = $process.StandardError.ReadToEnd()
        
        # Wait for the process to exit
        $process.WaitForExit()
        
        # Check exit code
        if ($process.ExitCode -eq 0) {
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
        } else {
            Write-Log "BCP process failed with exit code: $($process.ExitCode)" -Level ERROR
            Write-Log "BCP error output: $errorOutput" -Level ERROR
            return $false
        }
    } catch {
        Write-Log ("Error exporting data from ${fullTableName}`: {0}" -f $_.Exception.Message) -Level ERROR
        Write-Log "BCP error details: $($_.Exception.InnerException)" -Level ERROR
        return $false
    }
}

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
        
        # Use Start-Process instead of Invoke-Expression for better error handling
        $processStartInfo = New-Object System.Diagnostics.ProcessStartInfo
        $processStartInfo.FileName = "bcp"
        
        # Build arguments array
        $argList = @(
            "`"$fullTableName`"",
            "in",
            "`"$InputFile`"",
            $formatOptions,
            "-S", "`"$Server`"",
            "-d", "`"$Database`"",
            "-b", "$BatchSize"
        )
        
        if ($Credential) {
            $argList += "-U"
            $argList += "`"$($Credential.UserName)`""
            $argList += "-P"
            $argList += "`"$($Credential.GetNetworkCredential().Password)`""
        } else {
            $argList += "-T"
        }
        
        $argList += "-t"
        
        $processStartInfo.Arguments = $argList -join " "
        $processStartInfo.UseShellExecute = $false
        $processStartInfo.RedirectStandardOutput = $true
        $processStartInfo.RedirectStandardError = $true
        $processStartInfo.CreateNoWindow = $true
        
        $process = New-Object System.Diagnostics.Process
        $process.StartInfo = $processStartInfo
        
        # Log the BCP command (without password)
        $logCommand = $processStartInfo.Arguments
        if ($Credential) {
            $logCommand = $logCommand -replace " -P `"[^`"]+`"", " -P `"********`""
        }
        Write-Log "BCP command: bcp $logCommand" -Level INFO
        
        # Start the process
        $process.Start() | Out-Null
        
        # Capture output
        $output = $process.StandardOutput.ReadToEnd()
        $errorOutput = $process.StandardError.ReadToEnd()
        
        # Wait for the process to exit
        $process.WaitForExit()
        
        # Check exit code
        if ($process.ExitCode -eq 0) {
            Write-Log "Successfully imported data to ${fullTableName}" -Level INFO
            Write-Log "BCP output: $output" -Level INFO
            return $true
        } else {
            Write-Log "BCP process failed with exit code: $($process.ExitCode)" -Level ERROR
            Write-Log "BCP error output: $errorOutput" -Level ERROR
            return $false
        }
    } catch {
        Write-Log ("Error importing data to ${fullTableName}`: {0}" -f $_.Exception.Message) -Level ERROR
        Write-Log "BCP error details: $($_.Exception.InnerException)" -Level ERROR
        return $false
    }
}

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
            Write-Log "Exception details: $($_.Exception | Format-List -Force | Out-String)" -Level ERROR
            return @{ Success = $false }
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
        # Create result object to track statistics
        $result = @{
            Success = $false
            TableName = $fullTableName
            SourceRowCount = 0
            TargetRowCount = 0
            DataSizeBytes = 0
            StartTime = Get-Date
            EndTime = $null
            DurationSeconds = 0
            RowsPerSecond = 0
            MBPerSecond = 0
        }
        
        # Get source row count
        $sourceRowCount = Get-TableRowCount -Server $MigrationParams.SourceServer `
                                           -Database $MigrationParams.SourceDB `
                                           -Schema $schema `
                                           -Table $tableName `
                                           -Credential $MigrationParams.SourceCredential
        
        $result.SourceRowCount = $sourceRowCount
        Write-Log "Source row count for ${fullTableName}: $sourceRowCount" -Level INFO
        
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
            return $result
        }
        
        # Verify temp file was created
        if (-not (Test-Path $tempFilePath)) {
            Write-Log "Temp file was not created: $tempFilePath" -Level ERROR
            return $result
        }
        
        $fileInfo = Get-Item $tempFilePath
        $fileSizeBytes = $fileInfo.Length
        $result.DataSizeBytes = $fileSizeBytes
        
        $fileSizeMB = [Math]::Round($fileSizeBytes / 1MB, 2)
        Write-Log "Temp file created: $tempFilePath, Size: $fileSizeBytes bytes ($fileSizeMB MB)" -Level INFO
        
        $importResult = Import-TableData -Server $MigrationParams.TargetServer `
                                        -Database $MigrationParams.TargetDB `
                                        -Schema $schema `
                                        -Table $tableName `
                                        -InputFile $tempFilePath `
                                        -Format $MigrationParams.BCPFormat `
                                        -Credential $MigrationParams.TargetCredential
        
        if (-not $importResult) {
            Write-Log "Failed to import data to ${fullTableName}" -Level ERROR
            
            if (Test-Path $tempFilePath) {
                Remove-Item $tempFilePath -Force -ErrorAction SilentlyContinue
                Write-Log "Removed temporary file after error: $tempFilePath" -Level VERBOSE
            }
            
            return $result
        }
        
        # Get target row count for validation
        $targetRowCount = Get-TableRowCount -Server $MigrationParams.TargetServer `
                                           -Database $MigrationParams.TargetDB `
                                           -Schema $schema `
                                           -Table $tableName `
                                           -Credential $MigrationParams.TargetCredential
        
        $result.TargetRowCount = $targetRowCount
        Write-Log "Target row count for ${fullTableName}: $targetRowCount" -Level INFO
        
        # Validate row counts
        if ($sourceRowCount -ne $targetRowCount) {
            Write-Log "Row count mismatch for ${fullTableName}: Source=$sourceRowCount, Target=$targetRowCount" -Level WARNING
        } else {
            Write-Log "Row count validation successful for ${fullTableName}: $sourceRowCount rows" -Level INFO
        }
        
        # Calculate statistics
        $result.EndTime = Get-Date
        $result.DurationSeconds = [Math]::Round(($result.EndTime - $result.StartTime).TotalSeconds, 2)
        
        if ($result.DurationSeconds -gt 0 -and $sourceRowCount -gt 0) {
            $result.RowsPerSecond = [Math]::Round($sourceRowCount / $result.DurationSeconds, 2)
            $result.MBPerSecond = [Math]::Round(($fileSizeBytes / 1MB) / $result.DurationSeconds, 2)
            
            Write-Log "Migration statistics for ${fullTableName}:" -Level INFO
            Write-Log "  Duration: $($result.DurationSeconds) seconds" -Level INFO
            Write-Log "  Rows/second: $($result.RowsPerSecond)" -Level INFO
            Write-Log "  MB/second: $($result.MBPerSecond)" -Level INFO
        }
        
        if (Test-Path $tempFilePath) {
            Remove-Item $tempFilePath -Force
            Write-Log "Removed temporary file: $tempFilePath" -Level VERBOSE
        }
        
        $result.Success = $true
        return $result
    } catch {
        Write-Log ("Error migrating table ${fullTableName}`: {0}" -f $_.Exception.Message) -Level ERROR
        
        if (Test-Path $tempFilePath) {
            Remove-Item $tempFilePath -Force -ErrorAction SilentlyContinue
            Write-Log "Removed temporary file after error: $tempFilePath" -Level VERBOSE
        }
        
        return @{ 
            Success = $false
            TableName = $fullTableName
            Error = $_.Exception.Message
        }
    }
}

Export-ModuleMember -Function Test-BCPAvailability, Export-TableData, Import-TableData, Migrate-TableData
