# Constraints.psm1

function Get-ForeignKeyConstraints {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Server,
        
        [Parameter(Mandatory = $true)]
        [string]$Database,
        
        [Parameter(Mandatory = $false)]
        [string]$Authentication = "windows",
        
        [Parameter(Mandatory = $false)]
        [string]$Username = "",
        
        [Parameter(Mandatory = $false)]
        [string]$Password = ""
    )
    
    $query = @"
    SELECT
        OBJECT_SCHEMA_NAME(fk.parent_object_id) AS SchemaName,
        OBJECT_NAME(fk.parent_object_id) AS TableName,
        fk.name AS FKName,
        SCHEMA_NAME(pk_tab.schema_id) AS ReferencedSchemaName,
        pk_tab.name AS ReferencedTableName,
        CAST('ALTER TABLE [' + OBJECT_SCHEMA_NAME(fk.parent_object_id) + '].[' + 
        OBJECT_NAME(fk.parent_object_id) + '] DROP CONSTRAINT [' + fk.name + ']' AS NVARCHAR(MAX)) AS DropScript,
        CAST('ALTER TABLE [' + OBJECT_SCHEMA_NAME(fk.parent_object_id) + '].[' + 
        OBJECT_NAME(fk.parent_object_id) + '] WITH CHECK ADD CONSTRAINT [' + fk.name + 
        '] FOREIGN KEY(' + STUFF((SELECT ', [' + COL_NAME(fk.parent_object_id, fkc.parent_column_id) + ']'
        FROM sys.foreign_key_columns AS fkc
        WHERE fkc.constraint_object_id = fk.object_id
        ORDER BY fkc.constraint_column_id
        FOR XML PATH('')), 1, 2, '') + 
        ') REFERENCES [' + SCHEMA_NAME(pk_tab.schema_id) + '].[' + pk_tab.name + '] (' + 
        STUFF((SELECT ', [' + COL_NAME(fk.referenced_object_id, fkc.referenced_column_id) + ']'
        FROM sys.foreign_key_columns AS fkc
        WHERE fkc.constraint_object_id = fk.object_id
        ORDER BY fkc.constraint_column_id
        FOR XML PATH('')), 1, 2, '') + ')' AS NVARCHAR(MAX)) AS CreateScript
    FROM 
        sys.foreign_keys AS fk
        INNER JOIN sys.tables AS pk_tab ON fk.referenced_object_id = pk_tab.object_id
    ORDER BY
        SchemaName, TableName, FKName
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
    
    $constraints = Invoke-Sqlcmd @sqlParams
    return $constraints
}

function Get-TableDependencies {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Server,
        
        [Parameter(Mandatory = $true)]
        [string]$Database,
        
        [Parameter(Mandatory = $false)]
        [string]$Authentication = "windows",
        
        [Parameter(Mandatory = $false)]
        [string]$Username = "",
        
        [Parameter(Mandatory = $false)]
        [string]$Password = ""
    )
    
    $query = @"
    WITH TableDependencies AS (
        SELECT
            CAST(OBJECT_SCHEMA_NAME(fk.parent_object_id) + '.' + OBJECT_NAME(fk.parent_object_id) AS NVARCHAR(MAX)) AS DependentTable,
            CAST(OBJECT_SCHEMA_NAME(fk.referenced_object_id) + '.' + OBJECT_NAME(fk.referenced_object_id) AS NVARCHAR(MAX)) AS ReferencedTable
        FROM 
            sys.foreign_keys AS fk
    )
    SELECT DISTINCT * FROM TableDependencies
    ORDER BY ReferencedTable, DependentTable
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
    
    $dependencies = Invoke-Sqlcmd @sqlParams
    return $dependencies
}

Export-ModuleMember -Function Get-ForeignKeyConstraints, Get-TableDependencies
