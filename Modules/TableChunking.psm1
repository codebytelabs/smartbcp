# Table Chunking Module for SmartBCP
# Contains functions for analyzing tables and determining chunking strategies

function Get-TableSize {
    param (
        [string]$Server,
        [string]$Database,
        [string]$Schema,
        [string]$Table,
        [System.Management.Automation.PSCredential]$Credential
    )
    
    Write-Log "Getting size information for table [$Schema].[$Table]" -Level INFO
    
    $query = @"
    SELECT 
        t.NAME AS TableName,
        s.Name AS SchemaName,
        p.rows AS RowCounts,
        SUM(a.total_pages) * 8 AS TotalSpaceKB, 
        SUM(a.used_pages) * 8 AS UsedSpaceKB, 
        (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB
    FROM 
        sys.tables t
    INNER JOIN      
        sys.indexes i ON t.OBJECT_ID = i.object_id
    INNER JOIN 
        sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
    INNER JOIN 
        sys.allocation_units a ON p.partition_id = a.container_id
    INNER JOIN 
        sys.schemas s ON t.schema_id = s.schema_id
    WHERE 
        t.NAME = '$Table' AND s.Name = '$Schema'
        AND t.is_ms_shipped = 0
        AND i.OBJECT_ID > 255 
    GROUP BY 
        t.Name, s.Name, p.Rows
"@
    
    try {
        $sqlCommand = "sqlcmd -S `"$Server`" -d `"$Database`" -Q `"$query`" -h-1"
        
        if ($Credential) {
            $sqlCommand += " -U `"$($Credential.UserName)`" -P `"$($Credential.GetNetworkCredential().Password)`""
        } else {
            $sqlCommand += " -E"
        }
        
        $output = Invoke-Expression $sqlCommand -ErrorAction Stop
        
        # Parse the output to get the table size
        $tableSize = @{
            TableName = $Table
            SchemaName = $Schema
            RowCount = 0
            TotalSpaceMB = 0
            UsedSpaceMB = 0
        }
        
        # Parse the output
        $lines = $output -split "`n"
        foreach ($line in $lines) {
            if ($line -match '^\s*\S+\s+\S+\s+(\d+)\s+(\d+)\s+(\d+)') {
                $tableSize.RowCount = [int]$matches[1]
                $tableSize.TotalSpaceMB = [math]::Round([int]$matches[2] / 1024, 2)
                $tableSize.UsedSpaceMB = [math]::Round([int]$matches[3] / 1024, 2)
                break
            }
        }
        
        Write-Log "Table [$Schema].[$Table] has $($tableSize.RowCount) rows and uses $($tableSize.UsedSpaceMB) MB" -Level INFO
        return $tableSize
    } catch {
        Write-Log "Error getting table size for [$Schema].[$Table]: $($_.Exception.Message)" -Level ERROR
        return $null
    }
}

function Get-TableKeyColumn {
    param (
        [string]$Server,
        [string]$Database,
        [string]$Schema,
        [string]$Table,
        [System.Management.Automation.PSCredential]$Credential
    )
    
    Write-Log "Identifying key column for table [$Schema].[$Table]" -Level INFO
    
    # First try to get primary key
    $pkQuery = @"
    SELECT 
        c.name AS ColumnName,
        t.name AS DataType,
        c.max_length AS MaxLength,
        c.precision AS Precision,
        c.scale AS Scale,
        c.is_identity AS IsIdentity
    FROM 
        sys.indexes i
    INNER JOIN 
        sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN 
        sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    INNER JOIN 
        sys.types t ON c.user_type_id = t.user_type_id
    INNER JOIN 
        sys.tables tbl ON i.object_id = tbl.object_id
    INNER JOIN 
        sys.schemas s ON tbl.schema_id = s.schema_id
    WHERE 
        i.is_primary_key = 1
        AND tbl.name = '$Table'
        AND s.name = '$Schema'
    ORDER BY 
        ic.key_ordinal
"@
    
    try {
        $sqlCommand = "sqlcmd -S `"$Server`" -d `"$Database`" -Q `"$pkQuery`" -h-1"
        
        if ($Credential) {
            $sqlCommand += " -U `"$($Credential.UserName)`" -P `"$($Credential.GetNetworkCredential().Password)`""
        } else {
            $sqlCommand += " -E"
        }
        
        $output = Invoke-Expression $sqlCommand -ErrorAction Stop
        
        # Parse the output to get the primary key column
        $keyColumn = $null
        $lines = $output -split "`n"
        
        foreach ($line in $lines) {
            if ($line -match '^\s*(\S+)\s+(\S+)') {
                $columnName = $matches[1].Trim()
                $dataType = $matches[2].Trim()
                
                # Check if the data type is suitable for chunking (numeric or date types)
                $suitableTypes = @('int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 
                                   'datetime', 'datetime2', 'date', 'uniqueidentifier')
                
                if ($suitableTypes -contains $dataType) {
                    $keyColumn = @{
                        Name = $columnName
                        DataType = $dataType
                        IsPrimaryKey = $true
                        IsIdentity = $false
                    }
                    
                    # Check if it's also an identity column
                    if ($line -match '\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)') {
                        $keyColumn.IsIdentity = [bool]::Parse($matches[4])
                    }
                    
                    break
                }
            }
        }
        
        # If no suitable primary key, look for identity column
        if (-not $keyColumn) {
            $identityQuery = @"
            SELECT 
                c.name AS ColumnName,
                t.name AS DataType
            FROM 
                sys.columns c
            INNER JOIN 
                sys.types t ON c.user_type_id = t.user_type_id
            INNER JOIN 
                sys.tables tbl ON c.object_id = tbl.object_id
            INNER JOIN 
                sys.schemas s ON tbl.schema_id = s.schema_id
            WHERE 
                c.is_identity = 1
                AND tbl.name = '$Table'
                AND s.name = '$Schema'
"@
            
            $sqlCommand = "sqlcmd -S `"$Server`" -d `"$Database`" -Q `"$identityQuery`" -h-1"
            
            if ($Credential) {
                $sqlCommand += " -U `"$($Credential.UserName)`" -P `"$($Credential.GetNetworkCredential().Password)`""
            } else {
                $sqlCommand += " -E"
            }
            
            $output = Invoke-Expression $sqlCommand -ErrorAction Stop
            
            # Parse the output to get the identity column
            $lines = $output -split "`n"
            foreach ($line in $lines) {
                if ($line -match '^\s*(\S+)\s+(\S+)') {
                    $columnName = $matches[1].Trim()
                    $dataType = $matches[2].Trim()
                    
                    $keyColumn = @{
                        Name = $columnName
                        DataType = $dataType
                        IsPrimaryKey = $false
                        IsIdentity = $true
                    }
                    
                    break
                }
            }
        }
        
        if ($keyColumn) {
            Write-Log "Found key column for [$Schema].[$Table]: $($keyColumn.Name) ($($keyColumn.DataType))" -Level INFO
            if ($keyColumn.IsPrimaryKey) {
                Write-Log "Column is a primary key" -Level INFO
            }
            if ($keyColumn.IsIdentity) {
                Write-Log "Column is an identity column" -Level INFO
            }
        } else {
            Write-Log "No suitable key column found for [$Schema].[$Table]" -Level INFO
        }
        
        return $keyColumn
    } catch {
        Write-Log "Error identifying key column for [$Schema].[$Table]: $($_.Exception.Message)" -Level ERROR
        return $null
    }
}

function Get-KeyColumnRange {
    param (
        [string]$Server,
        [string]$Database,
        [string]$Schema,
        [string]$Table,
        [string]$KeyColumn,
        [System.Management.Automation.PSCredential]$Credential
    )
    
    Write-Log "Getting value range for key column [$KeyColumn] in table [$Schema].[$Table]" -Level INFO
    
    $query = @"
    SELECT 
        MIN([$KeyColumn]) AS MinValue,
        MAX([$KeyColumn]) AS MaxValue,
        COUNT([$KeyColumn]) AS RowCount
    FROM 
        [$Schema].[$Table]
"@
    
    try {
        $sqlCommand = "sqlcmd -S `"$Server`" -d `"$Database`" -Q `"$query`" -h-1"
        
        if ($Credential) {
            $sqlCommand += " -U `"$($Credential.UserName)`" -P `"$($Credential.GetNetworkCredential().Password)`""
        } else {
            $sqlCommand += " -E"
        }
        
        $output = Invoke-Expression $sqlCommand -ErrorAction Stop
        
        # Parse the output to get the min and max values
        $range = @{
            MinValue = $null
            MaxValue = $null
            RowCount = 0
        }
        
        $lines = $output -split "`n"
        foreach ($line in $lines) {
            if ($line -match '^\s*(\S+)\s+(\S+)\s+(\d+)') {
                $range.MinValue = $matches[1].Trim()
                $range.MaxValue = $matches[2].Trim()
                $range.RowCount = [int]$matches[3]
                break
            }
        }
        
        Write-Log "Key column [$KeyColumn] range: Min=$($range.MinValue), Max=$($range.MaxValue), Count=$($range.RowCount)" -Level INFO
        return $range
    } catch {
        Write-Log "Error getting key column range for [$Schema].[$Table].[$KeyColumn]: $($_.Exception.Message)" -Level ERROR
        return $null
    }
}

function Get-TableChunks {
    param (
        [string]$Server,
        [string]$Database,
        [string]$Schema,
        [string]$Table,
        [hashtable]$KeyColumn,
        [hashtable]$KeyRange,
        [int]$ChunkCount,
        [System.Management.Automation.PSCredential]$Credential
    )
    
    Write-Log "Calculating $ChunkCount chunks for table [$Schema].[$Table]" -Level INFO
    
    $chunks = @()
    
    # If we have a key column and range
    if ($KeyColumn -and $KeyRange -and $KeyRange.MinValue -ne $null -and $KeyRange.MaxValue -ne $null) {
        # For numeric data types
        $numericTypes = @('int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric')
        
        if ($numericTypes -contains $KeyColumn.DataType) {
            $minValue = [long]$KeyRange.MinValue
            $maxValue = [long]$KeyRange.MaxValue
            
            if ($minValue -eq $maxValue) {
                # Only one value, can't chunk
                $chunks += @{
                    ChunkId = 1
                    WhereClause = ""
                    RowEstimate = $KeyRange.RowCount
                }
            } else {
                $range = $maxValue - $minValue
                $chunkSize = [math]::Ceiling($range / $ChunkCount)
                
                for ($i = 0; $i -lt $ChunkCount; $i++) {
                    $chunkStart = $minValue + ($i * $chunkSize)
                    $chunkEnd = [math]::Min($minValue + (($i + 1) * $chunkSize) - 1, $maxValue)
                    
                    # Skip empty chunks
                    if ($chunkStart > $maxValue) {
                        continue
                    }
                    
                    $whereClause = "[$($KeyColumn.Name)] >= $chunkStart AND [$($KeyColumn.Name)] <= $chunkEnd"
                    $rowEstimate = [math]::Ceiling($KeyRange.RowCount / $ChunkCount)
                    
                    $chunks += @{
                        ChunkId = $i + 1
                        WhereClause = $whereClause
                        RowEstimate = $rowEstimate
                        StartValue = $chunkStart
                        EndValue = $chunkEnd
                    }
                }
            }
        }
        # For date/time data types
        elseif ($KeyColumn.DataType -match 'date|time') {
            # For date/time, we'll use row-based chunking as it's more reliable
            $chunks = Get-RowBasedChunks -Server $Server -Database $Database -Schema $Schema -Table $Table -ChunkCount $ChunkCount -Credential $Credential
        }
        # For uniqueidentifier
        elseif ($KeyColumn.DataType -eq 'uniqueidentifier') {
            # For GUIDs, we'll use row-based chunking as it's more reliable
            $chunks = Get-RowBasedChunks -Server $Server -Database $Database -Schema $Schema -Table $Table -ChunkCount $ChunkCount -Credential $Credential
        }
        else {
            # Fallback to row-based chunking for unsupported data types
            $chunks = Get-RowBasedChunks -Server $Server -Database $Database -Schema $Schema -Table $Table -ChunkCount $ChunkCount -Credential $Credential
        }
    } else {
        # No key column or range, use row-based chunking
        $chunks = Get-RowBasedChunks -Server $Server -Database $Database -Schema $Schema -Table $Table -ChunkCount $ChunkCount -Credential $Credential
    }
    
    Write-Log "Created $($chunks.Count) chunks for table [$Schema].[$Table]" -Level INFO
    return $chunks
}

function Get-RowBasedChunks {
    param (
        [string]$Server,
        [string]$Database,
        [string]$Schema,
        [string]$Table,
        [int]$ChunkCount,
        [System.Management.Automation.PSCredential]$Credential
    )
    
    Write-Log "Creating row-based chunks for table [$Schema].[$Table]" -Level INFO
    
    # Get total row count
    $countQuery = @"
    SELECT COUNT(*) AS RowCount FROM [$Schema].[$Table]
"@
    
    try {
        $sqlCommand = "sqlcmd -S `"$Server`" -d `"$Database`" -Q `"$countQuery`" -h-1"
        
        if ($Credential) {
            $sqlCommand += " -U `"$($Credential.UserName)`" -P `"$($Credential.GetNetworkCredential().Password)`""
        } else {
            $sqlCommand += " -E"
        }
        
        $output = Invoke-Expression $sqlCommand -ErrorAction Stop
        
        # Parse the output to get the row count
        $rowCount = 0
        $lines = $output -split "`n"
        foreach ($line in $lines) {
            if ($line -match '^\s*(\d+)') {
                $rowCount = [int]$matches[1]
                break
            }
        }
        
        if ($rowCount -eq 0) {
            # Empty table, return a single empty chunk
            return @(@{
                ChunkId = 1
                WhereClause = ""
                RowEstimate = 0
                IsRowBased = $true
            })
        }
        
        $chunks = @()
        $rowsPerChunk = [math]::Ceiling($rowCount / $ChunkCount)
        
        for ($i = 0; $i -lt $ChunkCount; $i++) {
            $startRow = ($i * $rowsPerChunk) + 1
            $endRow = [math]::Min(($i + 1) * $rowsPerChunk, $rowCount)
            
            # Skip empty chunks
            if ($startRow > $rowCount) {
                continue
            }
            
            $whereClause = "RowNum >= $startRow AND RowNum <= $endRow"
            
            $chunks += @{
                ChunkId = $i + 1
                WhereClause = $whereClause
                RowEstimate = $endRow - $startRow + 1
                StartRow = $startRow
                EndRow = $endRow
                IsRowBased = $true
            }
        }
        
        return $chunks
    } catch {
        Write-Log "Error creating row-based chunks for [$Schema].[$Table]: $($_.Exception.Message)" -Level ERROR
        
        # Return a single chunk as fallback
        return @(@{
            ChunkId = 1
            WhereClause = ""
            RowEstimate = 0
            IsRowBased = $true
        })
    }
}

function Get-OptimalChunkCount {
    param (
        [double]$TableSizeMB,
        [double]$MaxChunkSizeMB,
        [int]$MaxParallelChunks
    )
    
    # Calculate how many chunks we need based on table size
    $sizeBasedChunks = [math]::Ceiling($TableSizeMB / $MaxChunkSizeMB)
    
    # Limit by max parallel chunks
    $chunkCount = [math]::Min($sizeBasedChunks, $MaxParallelChunks)
    
    # Ensure at least 1 chunk
    $chunkCount = [math]::Max(1, $chunkCount)
    
    return $chunkCount
}

function Get-ChunkedExportQuery {
    param (
        [string]$Schema,
        [string]$Table,
        [hashtable]$Chunk
    )
    
    $fullTableName = "[$Schema].[$Table]"
    
    # For row-based chunks, we need a more complex query with ROW_NUMBER()
    if ($Chunk.IsRowBased) {
        $query = @"
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
    FROM $fullTableName
) AS RowNumberedTable
WHERE $($Chunk.WhereClause)
"@
    } else {
        # For key-based chunks, we can use a simple WHERE clause
        if ([string]::IsNullOrEmpty($Chunk.WhereClause)) {
            $query = "SELECT * FROM $fullTableName"
        } else {
            $query = "SELECT * FROM $fullTableName WHERE $($Chunk.WhereClause)"
        }
    }
    
    return $query
}

# Export module members
Export-ModuleMember -Function Get-TableSize, Get-TableKeyColumn, Get-KeyColumnRange, Get-TableChunks, Get-OptimalChunkCount, Get-ChunkedExportQuery
