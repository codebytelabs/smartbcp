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
        # Check if chunking is enabled and should be used for this table
        $useChunking = $false
        $chunks = @()
        
        if ($MigrationParams.EnableChunking -eq $true) {
            # Import the TableChunking module
            Import-Module (Join-Path $PSScriptRoot "TableChunking.psm1") -Force
            
            # Get table size
            $tableSize = Get-TableSize -Server $MigrationParams.SourceServer `
                                      -Database $MigrationParams.SourceDB `
                                      -Schema $schema `
                                      -Table $tableName `
                                      -Credential $MigrationParams.SourceCredential
            
            # Check if table size exceeds the chunking threshold
            if ($tableSize -and $tableSize.UsedSpaceMB -ge $MigrationParams.ChunkingThresholdMB) {
                Write-Log "Table size ($($tableSize.UsedSpaceMB) MB) exceeds chunking threshold ($($MigrationParams.ChunkingThresholdMB) MB)" -Level INFO
                Write-Log "Using chunked migration for table $fullTableName" -Level INFO
                
                # Get key column for chunking
                $keyColumn = Get-TableKeyColumn -Server $MigrationParams.SourceServer `
                                              -Database $MigrationParams.SourceDB `
                                              -Schema $schema `
                                              -Table $tableName `
                                              -Credential $MigrationParams.SourceCredential
                
                if ($keyColumn) {
                    # Get key column range
                    $keyRange = Get-KeyColumnRange -Server $MigrationParams.SourceServer `
                                                 -Database $MigrationParams.SourceDB `
                                                 -Schema $schema `
                                                 -Table $tableName `
                                                 -KeyColumn $keyColumn.Name `
                                                 -Credential $MigrationParams.SourceCredential
                    
                    # Calculate optimal chunk count
                    $maxParallelChunks = [math]::Max(1, $MigrationParams.ParallelTasks * 2)
                    $chunkCount = Get-OptimalChunkCount -TableSizeMB $tableSize.UsedSpaceMB `
                                                      -MaxChunkSizeMB $MigrationParams.MaxChunkSizeMB `
                                                      -MaxParallelChunks $maxParallelChunks
                    
                    Write-Log "Creating $chunkCount chunks for table $fullTableName" -Level INFO
                    
                    # Get table chunks
                    $chunks = Get-TableChunks -Server $MigrationParams.SourceServer `
                                            -Database $MigrationParams.SourceDB `
                                            -Schema $schema `
                                            -Table $tableName `
                                            -KeyColumn $keyColumn `
                                            -KeyRange $keyRange `
                                            -ChunkCount $chunkCount `
                                            -Credential $MigrationParams.SourceCredential
                    
                    if ($chunks -and $chunks.Count -gt 1) {
                        $useChunking = $true
                        Write-Log "Will use $($chunks.Count) chunks for table $fullTableName" -Level INFO
                    } else {
                        Write-Log "Chunking not effective for table $fullTableName, using single file" -Level INFO
                    }
                } else {
                    Write-Log "No suitable key column found for chunking table $fullTableName, using single file" -Level INFO
                }
            } else {
                Write-Log "Table size is below chunking threshold, using single file" -Level INFO
            }
        }
        
        # If chunking is enabled and chunks were created, use chunked migration
        if ($useChunking -and $chunks.Count -gt 0) {
            $timestamp = Get-Date -Format 'yyyyMMddHHmmss'
            $chunkResults = @()
            $chunkFiles = @()
            
            # Export and import each chunk
            foreach ($chunk in $chunks) {
                $chunkId = $chunk.ChunkId
                $tempFileName = "$schema`_$tableName`_${timestamp}_Chunk$($chunkId.ToString('000')).dat"
                $tempFilePath = Join-Path $MigrationParams.TempFilePath $tempFileName
                
                Write-Log "Processing chunk $chunkId of $($chunks.Count) for table $fullTableName" -Level INFO
                Write-Log "Temp file path for chunk: $tempFilePath" -Level INFO
                
                # For row-based chunks, we need to use a query
                if ($chunk.IsRowBased) {
                    $query = Get-ChunkedExportQuery -Schema $schema -Table $tableName -Chunk $chunk
                    
                    Write-Log "Using row-based chunking with query: $(if ($query.Length -gt 100) { $query.Substring(0, 100) + '...' } else { $query })" -Level INFO
                    
                    # Export chunk using query
                    $exportResult = Export-QueryData -Server $MigrationParams.SourceServer `
                                                   -Database $MigrationParams.SourceDB `
                                                   -Query $query `
                                                   -OutputFile $tempFilePath `
                                                   -Format $MigrationParams.BCPFormat `
                                                   -Credential $MigrationParams.SourceCredential
                } else {
                    # For key-based chunks, we can use a WHERE clause
                    $whereClause = $chunk.WhereClause
                    
                    if ([string]::IsNullOrEmpty($whereClause)) {
                        Write-Log "Using key-based chunking with no WHERE clause (full table)" -Level INFO
                        
                        # Export chunk using standard export
                        $exportResult = Export-TableData -Server $MigrationParams.SourceServer `
                                                       -Database $MigrationParams.SourceDB `
                                                       -Schema $schema `
                                                       -Table $tableName `
                                                       -OutputFile $tempFilePath `
                                                       -Format $MigrationParams.BCPFormat `
                                                       -Credential $MigrationParams.SourceCredential
                    } else {
                        Write-Log "Using key-based chunking with WHERE clause: $($whereClause)" -Level INFO
                        
                        # Export chunk using query
                        $query = "SELECT * FROM [$schema].[$tableName] WHERE " + $whereClause
                        $exportResult = Export-QueryData -Server $MigrationParams.SourceServer `
                                                       -Database $MigrationParams.SourceDB `
                                                       -Query $query `
                                                       -OutputFile $tempFilePath `
                                                       -Format $MigrationParams.BCPFormat `
                                                       -Credential $MigrationParams.SourceCredential
                    }
                }
                
                if (-not $exportResult) {
                    Write-Log "Failed to export chunk $chunkId for table $fullTableName" -Level ERROR
                    $chunkResults += $false
                    continue
                }
                
                # Verify temp file was created
                if (-not (Test-Path $tempFilePath)) {
                    Write-Log "Temp file was not created for chunk $chunkId - $tempFilePath" -Level ERROR
                    $chunkResults += $false
                    continue
                }
                
                $fileInfo = Get-Item $tempFilePath
                Write-Log "Temp file created for chunk $chunkId - $tempFilePath, Size: $($fileInfo.Length) bytes" -Level INFO
                
                # Add file to list of chunk files
                $chunkFiles += $tempFilePath
                
                # Import the chunk
                $importResult = Import-TableData -Server $MigrationParams.TargetServer `
                                               -Database $MigrationParams.TargetDB `
                                               -Schema $schema `
                                               -Table $tableName `
                                               -InputFile $tempFilePath `
                                               -Format $MigrationParams.BCPFormat `
                                               -BatchSize $MigrationParams.BatchSize `
                                               -Credential $MigrationParams.TargetCredential
                
                if (-not $importResult) {
                    Write-Log "Failed to import chunk $chunkId to table $fullTableName" -Level ERROR
                    $chunkResults += $false
                } else {
                    Write-Log "Successfully imported chunk $chunkId to table $fullTableName" -Level INFO
                    $chunkResults += $true
                }
                
                # Clean up temp file
                if (Test-Path $tempFilePath) {
                    Remove-Item $tempFilePath -Force
                    Write-Log "Removed temporary file for chunk $chunkId - $tempFilePath" -Level VERBOSE
                }
            }
            
            # Check if all chunks were successful
            $allChunksSuccessful = ($chunkResults | Where-Object { $_ -eq $false }).Count -eq 0
            
            if ($allChunksSuccessful) {
                Write-Log "All $($chunks.Count) chunks successfully migrated for table $fullTableName" -Level INFO
                return $true
            } else {
                $failedChunks = ($chunkResults | Where-Object { $_ -eq $false }).Count
                Write-Log "$failedChunks out of $($chunks.Count) chunks failed to migrate for table $fullTableName" -Level ERROR
                return $false
            }
        } else {
            # Use standard migration (single file)
            $tempFileName = "$schema`_$tableName`_$(Get-Date -Format 'yyyyMMddHHmmss').dat"
            $tempFilePath = Join-Path $MigrationParams.TempFilePath $tempFileName
            
            Write-Log "Using standard migration (single file) for table $fullTableName" -Level INFO
            Write-Log "Temp file path: $tempFilePath" -Level INFO
            
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
                                           -BatchSize $MigrationParams.BatchSize `
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
        }
    } catch {
        Write-Log ("Error migrating table ${fullTableName}`: {0}" -f $_.Exception.Message) -Level ERROR
        
        # Clean up any temp files
        $tempFiles = Get-ChildItem -Path (Join-Path $MigrationParams.TempFilePath "$schema`_$tableName`_*.dat") -ErrorAction SilentlyContinue
        foreach ($file in $tempFiles) {
            try {
                Remove-Item $file.FullName -Force -ErrorAction SilentlyContinue
                Write-Log "Removed temporary file after error: $($file.FullName)" -Level VERBOSE
            } catch {
                # Ignore errors during cleanup
            }
        }
        
        return $false
    }
}

function Export-QueryData {
    param (
        [string]$Server,
        [string]$Database,
        [string]$Query,
        [string]$OutputFile,
        [string]$Format = "native",
        [System.Management.Automation.PSCredential]$Credential
    )
    
    Write-Log "Export-QueryData called with parameters:" -Level INFO
    Write-Log "  Server: $Server" -Level INFO
    Write-Log "  Database: $Database" -Level INFO
    Write-Log "  Query: $Query" -Level INFO
    Write-Log "  OutputFile: $OutputFile" -Level INFO
    Write-Log "  Format: $Format" -Level INFO
    Write-Log "  Credential: $(if ($Credential) { "Provided" } else { "Not provided" })" -Level INFO
    
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
    }
    
    # Create a temporary query file
    $queryFile = Join-Path $outputDir "temp_query_$(Get-Random).sql"
    try {
        Set-Content -Path $queryFile -Value $Query -Encoding UTF8
        
        # Build the BCP command
        $bcpCommand = "bcp `"$Query`" queryout `"$OutputFile`" $formatOptions -S `"$Server`" -d `"$Database`""
        
        if ($Credential) {
            $bcpCommand += " -U `"$($Credential.UserName)`" -P `"$($Credential.GetNetworkCredential().Password)`""
        } else {
            $bcpCommand += " -T"
        }
        
        $bcpCommand += " -t -q"
        
        # Log the BCP command (without password)
        $logCommand = $bcpCommand
        if ($Credential) {
            $logCommand = $bcpCommand -replace " -P `"[^`"]+`"", " -P `"********`""
        }
        Write-Log "BCP command: $logCommand" -Level INFO
        
        try {
            Write-Log "Exporting data using query to $OutputFile..." -Level INFO
            
            # Use Start-Process instead of Invoke-Expression for better error handling
            $processStartInfo = New-Object System.Diagnostics.ProcessStartInfo
            $processStartInfo.FileName = "bcp"
            
            # Build arguments array
            $argList = @(
                "`"$Query`"",
                "queryout",
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
            $argList += "-q"
            
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
                Write-Log "Successfully exported data using query" -Level INFO
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
            Write-Log ("Error exporting data using query: {0}" -f $_.Exception.Message) -Level ERROR
            Write-Log "BCP error details: $($_.Exception.InnerException)" -Level ERROR
            return $false
        }
    } finally {
        # Clean up the temporary query file
        if (Test-Path $queryFile) {
            Remove-Item $queryFile -Force -ErrorAction SilentlyContinue
        }
    }
}

Export-ModuleMember -Function Test-BCPAvailability, Export-TableData, Import-TableData, Migrate-TableData, Export-QueryData
