# Configuration.psm1

function Import-SmartBcpConfig {
    param (
        [Parameter(Mandatory = $true)]
        [string]$ConfigFile
    )
    
    if (-not (Test-Path -Path $ConfigFile)) {
        throw "Configuration file not found: $ConfigFile"
    }
    
    try {
        $config = Get-Content -Path $ConfigFile -Raw | ConvertFrom-Json
        
        # Validate required configuration elements
        if (-not $config.source -or -not $config.source.server -or -not $config.source.database) {
            throw "Source configuration is incomplete"
        }
        
        if (-not $config.destination -or -not $config.destination.server -or -not $config.destination.database) {
            throw "Destination configuration is incomplete"
        }
        
        if (-not $config.tables -or $config.tables.Count -eq 0) {
            throw "No tables specified for processing"
        }
        
        # Set default values for optional parameters
        if (-not $config.options) {
            $config | Add-Member -MemberType NoteProperty -Name "options" -Value @{}
        }
        
        if (-not $config.options.maxThreads) {
            $config.options | Add-Member -MemberType NoteProperty -Name "maxThreads" -Value 8
        }
        
        if (-not $config.options.batchSize) {
            $config.options | Add-Member -MemberType NoteProperty -Name "batchSize" -Value 10000
        }
        
        if (-not $config.options.tempFolder) {
            $tempPath = Join-Path -Path $env:TEMP -ChildPath "SmartBCP"
            $config.options | Add-Member -MemberType NoteProperty -Name "tempFolder" -Value $tempPath
        }
        
        if (-not (Get-Member -InputObject $config.options -Name "truncateTarget" -MemberType Properties)) {
            $config.options | Add-Member -MemberType NoteProperty -Name "truncateTarget" -Value $true
        }
        
        return $config
    }
    catch {
        throw "Error parsing configuration file: $_"
    }
}

function Expand-TableWildcards {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Server,
        
        [Parameter(Mandatory = $true)]
        [string]$Database,
        
        [Parameter(Mandatory = $true)]
        [array]$TablePatterns
    )
    
    $expandedTables = @()
    
    foreach ($pattern in $TablePatterns) {
        if ($pattern -match '^\s*(\[?[\w\d]+\]?)\.(\*|\[?[\w\d]+\]?)$') {
            $schema = $matches[1] -replace '^\[|\]$', ''
            $tableName = $matches[2]
            
            if ($tableName -eq "*") {
                # Get all tables in the specified schema
                $query = @"
                SELECT 
                    CONCAT(SCHEMA_NAME(schema_id), '.', name) AS TableName
                FROM 
                    sys.tables
                WHERE 
                    SCHEMA_NAME(schema_id) = '$schema'
                ORDER BY 
                    name
"@
                
                $schemaTables = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query | 
                               Select-Object -ExpandProperty TableName
                
                $expandedTables += $schemaTables
            }
            else {
                # Add the specific table
                $expandedTables += $pattern
            }
        }
        else {
            # Assume it's a fully qualified table name
            $expandedTables += $pattern
        }
    }
    
    return $expandedTables
}

function Get-TableProcessingOrder {
    param (
        [Parameter(Mandatory = $true)]
        [array]$Tables,
        
        [Parameter(Mandatory = $true)]
        [array]$Dependencies
    )
    
    # Create a dependency graph
    $graph = @{}
    foreach ($table in $Tables) {
        $graph[$table] = @()
    }
    
    # Populate dependencies
    foreach ($dep in $Dependencies) {
        $dependent = $dep.DependentTable
        $referenced = $dep.ReferencedTable
        
        # Only include tables that are in our processing list
        if ($Tables -contains $dependent -and $Tables -contains $referenced) {
            $graph[$dependent] += $referenced
        }
    }
    
    # Helper function for topological sort
    function Visit-Node {
        param($node, $visited, $temp, $graph, [ref]$result)
        
        # Return if node is already processed
        if ($visited -contains $node) { return }
        
        # Check for circular dependency
        if ($temp -contains $node) {
            throw "Circular dependency detected involving table $node"
        }
        
        $temp += $node
        
        # Visit all dependencies
        foreach ($dependency in $graph[$node]) {
            Visit-Node -node $dependency -visited $visited -temp $temp -graph $graph -result $result
        }
        
        $visited += $node
        $result.Value = @($node) + $result.Value
    }
    
    # Perform topological sort
    $visited = @()
    $result = @()
    
    foreach ($table in $graph.Keys) {
        if ($visited -notcontains $table) {
            Visit-Node -node $table -visited $visited -temp @() -graph $graph -result ([ref]$result)
        }
    }
    
    return $result
}

Export-ModuleMember -Function Import-SmartBcpConfig, Expand-TableWildcards, Get-TableProcessingOrder
