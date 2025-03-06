# Configuration-Enhanced.psm1
# Enhanced version with support for automatic schema discovery

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
        
        # Handle relative paths for tempFolder
        if ($config.options.tempFolder) {
            # If path is relative, make it absolute based on script location
            if (-not [System.IO.Path]::IsPathRooted($config.options.tempFolder)) {
                # Get the config file directory
                $configDir = Split-Path -Parent $ConfigFile
                
                # Log the paths for debugging
                Write-Verbose "Config file directory: $configDir"
                Write-Verbose "Relative temp folder: $($config.options.tempFolder)"
                
                # Make path absolute without using Join-Path to avoid path duplication
                if ($config.options.tempFolder.StartsWith('.\') -or $config.options.tempFolder.StartsWith('./')) {
                    $relativePath = $config.options.tempFolder.Substring(2)
                    $absolutePath = "$configDir\$relativePath"
                } else {
                    $absolutePath = "$configDir\$($config.options.tempFolder)"
                }
                
                Write-Verbose "Absolute temp folder path: $absolutePath"
                $config.options.tempFolder = $absolutePath
            }
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
        [array]$TablePatterns,
        
        [Parameter(Mandatory = $false)]
        [string]$Authentication = "windows",
        
        [Parameter(Mandatory = $false)]
        [string]$Username = "",
        
        [Parameter(Mandatory = $false)]
        [string]$Password = ""
    )
    
    $expandedTables = @()
    
    # Check if the special "ALL" value is present
    if ($TablePatterns -contains "ALL") {
        # Get all schemas
        $schemasQuery = @"
        SELECT DISTINCT 
            SCHEMA_NAME(schema_id) AS SchemaName
        FROM 
            sys.tables
        ORDER BY 
            SchemaName
"@
        
        # Set SQL authentication parameters if needed
        $sqlParams = @{
            ServerInstance = $Server
            Database = $Database
            Query = $schemasQuery
        }
        
        if ($Authentication -eq "sql") {
            $sqlParams.Add("Username", $Username)
            $sqlParams.Add("Password", $Password)
        }
        
        $schemas = Invoke-Sqlcmd @sqlParams | Select-Object -ExpandProperty SchemaName
        
        # For each schema, get all tables
        foreach ($schema in $schemas) {
            $tablesQuery = @"
            SELECT 
                CONCAT('$schema', '.', name) AS TableName
            FROM 
                sys.tables
            WHERE 
                SCHEMA_NAME(schema_id) = '$schema'
            ORDER BY 
                name
"@
            
            $sqlParams = @{
                ServerInstance = $Server
                Database = $Database
                Query = $tablesQuery
            }
            
            if ($Authentication -eq "sql") {
                $sqlParams.Add("Username", $Username)
                $sqlParams.Add("Password", $Password)
            }
            
            $schemaTables = Invoke-Sqlcmd @sqlParams | Select-Object -ExpandProperty TableName
            
            $expandedTables += $schemaTables
        }
        
        Write-Verbose "Expanded 'ALL' to $($expandedTables.Count) tables across $($schemas.Count) schemas"
        return $expandedTables
    }
    
    # Process regular patterns
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
                
                $sqlParams = @{
                    ServerInstance = $Server
                    Database = $Database
                    Query = $query
                }
                
                if ($Authentication -eq "sql") {
                    $sqlParams.Add("Username", $Username)
                    $sqlParams.Add("Password", $Password)
                }
                
                $schemaTables = Invoke-Sqlcmd @sqlParams | Select-Object -ExpandProperty TableName
                
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
    
    # Track circular dependencies
    $circularDependencies = @{}
    
    # Helper function for topological sort with circular dependency detection
    function Visit-Node {
        param(
            $node, 
            $visited, 
            $temp, 
            $graph, 
            [ref]$result, 
            [ref]$circularDeps
        )
        
        # Return if node is already processed
        if ($visited -contains $node) { return $true }
        
        # Check for circular dependency
        if ($temp -contains $node) {
            # Find the cycle
            $cycleStart = [array]::IndexOf($temp, $node)
            $cycle = $temp[$cycleStart..$temp.Count]
            
            # Record the circular dependency
            $cycleKey = ($cycle -join " -> ") + " -> $node"
            $circularDeps.Value[$cycleKey] = $cycle
            
            # Return false to indicate circular dependency
            return $false
        }
        
        $temp += $node
        
        # Visit all dependencies
        $hasCycle = $false
        $nodeDependencies = @($graph[$node])  # Create a copy to avoid modifying during iteration
        
        foreach ($dependency in $nodeDependencies) {
            $success = Visit-Node -node $dependency -visited $visited -temp $temp -graph $graph -result $result -circularDeps $circularDeps
            if (-not $success) {
                $hasCycle = $true
                # Don't break here, continue to find all cycles
            }
        }
        
        $visited += $node
        $result.Value = @($node) + $result.Value
        
        return -not $hasCycle
    }
    
    # First pass: detect circular dependencies
    $visited = @()
    $result = @()
    $circularDeps = @{}
    
    foreach ($table in $graph.Keys) {
        if ($visited -notcontains $table) {
            Visit-Node -node $table -visited $visited -temp @() -graph $graph -result ([ref]$result) -circularDeps ([ref]$circularDependencies)
        }
    }
    
    # If circular dependencies were found, break them and try again
    if ($circularDependencies.Count -gt 0) {
        Write-Warning "Circular dependencies detected in the following tables:"
        foreach ($cycle in $circularDependencies.Keys) {
            Write-Warning "  $cycle"
            
            # Break the cycle by removing the last dependency in the cycle
            $cycleArray = $circularDependencies[$cycle]
            $lastNode = $cycleArray[-1]
            $firstNode = $cycleArray[0]
            
            # Remove the dependency from last node to first node
            $graph[$lastNode] = $graph[$lastNode] | Where-Object { $_ -ne $firstNode }
            
            Write-Warning "  Breaking dependency: $lastNode -> $firstNode"
        }
        
        # Second pass: topological sort with broken cycles
        $visited = @()
        $result = @()
        
        # Simplified Visit-Node function without cycle detection for second pass
        function Visit-Node-Simple {
            param($node, $visited, $graph, [ref]$result)
            
            # Return if node is already processed
            if ($visited -contains $node) { return }
            
            $visited += $node
            
            # Visit all dependencies
            foreach ($dependency in $graph[$node]) {
                Visit-Node-Simple -node $dependency -visited $visited -graph $graph -result $result
            }
            
            $result.Value = @($node) + $result.Value
        }
        
        foreach ($table in $graph.Keys) {
            if ($visited -notcontains $table) {
                Visit-Node-Simple -node $table -visited $visited -graph $graph -result ([ref]$result)
            }
        }
    }
    
    return $result
}

Export-ModuleMember -Function Import-SmartBcpConfig, Expand-TableWildcards, Get-TableProcessingOrder
