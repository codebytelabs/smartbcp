# TableInfo.psm1

function Get-TablePartitions {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Server,
        
        [Parameter(Mandatory = $true)]
        [string]$Database,
        
        [Parameter(Mandatory = $true)]
        [string]$TableName,
        
        [Parameter(Mandatory = $false)]
        [string]$Authentication = "windows",
        
        [Parameter(Mandatory = $false)]
        [string]$Username = "",
        
        [Parameter(Mandatory = $false)]
        [string]$Password = ""
    )
    
    $schemaTable = $TableName.Split('.')
    $schema = $schemaTable[0]
    $table = $schemaTable[1]
    
    # Check if table is partitioned
    $isPartitionedQuery = @"
    SELECT COUNT(*) AS IsPartitioned
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
    WHERE SCHEMA_NAME(t.schema_id) = '$schema' AND t.name = '$table' AND i.index_id <= 1
"@
    
    # Set SQL authentication parameters if needed
    $sqlParams = @{
        ServerInstance = $Server
        Database = $Database
        Query = $isPartitionedQuery
    }
    
    if ($Authentication -eq "sql") {
        $sqlParams.Add("Username", $Username)
        $sqlParams.Add("Password", $Password)
    }
    
    $isPartitioned = (Invoke-Sqlcmd @sqlParams).IsPartitioned
    
    if ($isPartitioned -eq 0) {
        # Not partitioned, return single partition info
        return @{ "IsPartitioned" = $false; "Partitions" = @(1) }
    }
    
    # Get partition function and column
    $partitionInfoQuery = @"
    SELECT 
        pf.name AS PartitionFunction,
        pc.name AS PartitionColumn
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
    INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN sys.columns pc ON t.object_id = pc.object_id AND ic.column_id = pc.column_id
    WHERE SCHEMA_NAME(t.schema_id) = '$schema' 
        AND t.name = '$table' 
        AND i.index_id <= 1
        AND ic.partition_ordinal = 1
"@
    
    $sqlParams = @{
        ServerInstance = $Server
        Database = $Database
        Query = $partitionInfoQuery
    }
    
    if ($Authentication -eq "sql") {
        $sqlParams.Add("Username", $Username)
        $sqlParams.Add("Password", $Password)
    }
    
    $partitionInfo = Invoke-Sqlcmd @sqlParams
    
    # Get all partition numbers
    $partitionNumbersQuery = @"
    SELECT DISTINCT p.partition_number
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    WHERE SCHEMA_NAME(t.schema_id) = '$schema' AND t.name = '$table' AND i.index_id <= 1
    ORDER BY p.partition_number
"@
    
    $sqlParams = @{
        ServerInstance = $Server
        Database = $Database
        Query = $partitionNumbersQuery
    }
    
    if ($Authentication -eq "sql") {
        $sqlParams.Add("Username", $Username)
        $sqlParams.Add("Password", $Password)
    }
    
    $partitionNumbers = (Invoke-Sqlcmd @sqlParams) | Select-Object -ExpandProperty partition_number
    
    return @{
        "IsPartitioned" = $true
        "Partitions" = $partitionNumbers
        "PartitionFunction" = $partitionInfo.PartitionFunction
        "PartitionColumn" = $partitionInfo.PartitionColumn
    }
}

function Get-TableColumnInfo {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Server,
        
        [Parameter(Mandatory = $true)]
        [string]$Database,
        
        [Parameter(Mandatory = $true)]
        [string]$TableName,
        
        [Parameter(Mandatory = $false)]
        [string]$Authentication = "windows",
        
        [Parameter(Mandatory = $false)]
        [string]$Username = "",
        
        [Parameter(Mandatory = $false)]
        [string]$Password = ""
    )
    
    $schemaTable = $TableName.Split('.')
    $schema = $schemaTable[0]
    $table = $schemaTable[1]
    
    $query = @"
    SELECT 
        c.name AS ColumnName,
        t.name AS DataType,
        c.max_length AS MaxLength,
        c.precision AS Precision,
        c.scale AS Scale,
        c.is_nullable AS IsNullable,
        c.is_identity AS IsIdentity
    FROM 
        sys.columns c
        INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
        INNER JOIN sys.tables tbl ON c.object_id = tbl.object_id
    WHERE 
        SCHEMA_NAME(tbl.schema_id) = '$schema'
        AND tbl.name = '$table'
    ORDER BY 
        c.column_id
"@
    
    # Set SQL authentication parameters if needed
    $sqlParams = @{
        ServerInstance = $Server
        Database = $Database
        Query = $query
    }
    
    if ($Authentication -eq "sql") {
        $sqlParams.Add("Username", $Username)
        $sqlParams.Add("Password", $Password)
    }
    
    $columns = Invoke-Sqlcmd @sqlParams
    return $columns
}

Export-ModuleMember -Function Get-TablePartitions, Get-TableColumnInfo
