# Configuration Module for SmartBCP
# Contains functions for configuration and parameter handling

# Load Configuration from File if provided
function Load-Configuration {
    param(
        [string]$ConfigFilePath
    )
    
    if (-not (Test-Path $ConfigFilePath)) {
        Write-Log "Configuration file not found: $ConfigFilePath" -Level ERROR
        throw "Configuration file not found: $ConfigFilePath"
    }
    
    try {
        $config = Get-Content $ConfigFilePath -Raw | ConvertFrom-Json
        Write-Log "Loaded configuration from $ConfigFilePath" -Level INFO
        
        # Validate required configuration properties
        $requiredProps = @("sourceServer", "sourceDB", "targetServer", "targetDB")
        $missingProps = @()
        
        foreach ($prop in $requiredProps) {
            if (-not (Get-Member -InputObject $config -Name $prop -MemberType Properties)) {
                $missingProps += $prop
            }
        }
        
        if ($missingProps.Count -gt 0) {
            Write-Log "Missing required configuration properties: $($missingProps -join ', ')" -Level ERROR
            throw "Missing required configuration properties: $($missingProps -join ', ')"
        }
        
        return $config
    } catch {
        Write-Log "Error loading configuration: $($_.Exception.Message)" -Level ERROR
        throw "Error loading configuration: $($_.Exception.Message)"
    }
}

# Convert configuration to parameters for Start-SmartBCP
function Convert-ConfigToParameters {
    param(
        [Parameter(Mandatory=$true)]
        [PSObject]$Config
    )
    
    $params = @{
        SourceServer = $Config.sourceServer
        SourceDB = $Config.sourceDB
        TargetServer = $Config.targetServer
        TargetDB = $Config.targetDB
    }
    
    # Handle optional parameters
    if (Get-Member -InputObject $Config -Name "parallelTasks" -MemberType Properties) {
        $params.Add("ParallelTasks", $Config.parallelTasks)
    }
    
    if (Get-Member -InputObject $Config -Name "bcpFormat" -MemberType Properties) {
        $params.Add("BCPFormat", $Config.bcpFormat)
    }
    
    if (Get-Member -InputObject $Config -Name "tempPath" -MemberType Properties) {
        $params.Add("TempPath", $Config.tempPath)
    }
    
    if (Get-Member -InputObject $Config -Name "manageForeignKeys" -MemberType Properties) {
        $params.Add("ManageForeignKeys", $Config.manageForeignKeys)
    }
    
    if (Get-Member -InputObject $Config -Name "truncateTargetTables" -MemberType Properties) {
        $params.Add("TruncateTargetTables", $Config.truncateTargetTables)
    }
    
    if (Get-Member -InputObject $Config -Name "batchSize" -MemberType Properties) {
        $params.Add("BatchSize", $Config.batchSize)
    }
    
    # Handle chunking parameters
    if (Get-Member -InputObject $Config -Name "enableChunking" -MemberType Properties) {
        $params.Add("EnableChunking", $Config.enableChunking)
    }
    
    if (Get-Member -InputObject $Config -Name "maxChunkSizeMB" -MemberType Properties) {
        $params.Add("MaxChunkSizeMB", $Config.maxChunkSizeMB)
    }
    
    if (Get-Member -InputObject $Config -Name "chunkingThresholdMB" -MemberType Properties) {
        $params.Add("ChunkingThresholdMB", $Config.chunkingThresholdMB)
    }
    
    if ((Get-Member -InputObject $Config -Name "includeSchemas" -MemberType Properties) -and ($Config.includeSchemas.Count -gt 0)) {
        $params.Add("IncludeSchemas", $Config.includeSchemas)
    }
    
    if ((Get-Member -InputObject $Config -Name "excludeSchemas" -MemberType Properties) -and ($Config.excludeSchemas.Count -gt 0)) {
        $params.Add("ExcludeSchemas", $Config.excludeSchemas)
    }
    
    if ((Get-Member -InputObject $Config -Name "includeTables" -MemberType Properties) -and ($Config.includeTables.Count -gt 0)) {
        $params.Add("IncludeTables", $Config.includeTables)
    }
    
    if ((Get-Member -InputObject $Config -Name "excludeTables" -MemberType Properties) -and ($Config.excludeTables.Count -gt 0)) {
        $params.Add("ExcludeTables", $Config.excludeTables)
    }
    
    # Handle authentication
    if (Get-Member -InputObject $Config -Name "authentication" -MemberType Properties) {
        $auth = $Config.authentication
        
        if (Get-Member -InputObject $auth -Name "type" -MemberType Properties) {
            if ($auth.type -eq "sql") {
                # Check for separate source and target credentials
                if (Get-Member -InputObject $auth -Name "source" -MemberType Properties) {
                    $sourceAuth = $auth.source
                    if ((Get-Member -InputObject $sourceAuth -Name "username" -MemberType Properties) -and 
                        (Get-Member -InputObject $sourceAuth -Name "password" -MemberType Properties)) {
                        
                        $securePassword = ConvertTo-SecureString $sourceAuth.password -AsPlainText -Force
                        $sourceCredential = New-Object System.Management.Automation.PSCredential($sourceAuth.username, $securePassword)
                        
                        $params.Add("SourceCredential", $sourceCredential)
                        Write-Log "Using SQL authentication for source from config file" -Level INFO
                    } else {
                        Write-Log "Source SQL authentication specified but username or password missing" -Level WARNING
                    }
                }
                
                if (Get-Member -InputObject $auth -Name "target" -MemberType Properties) {
                    $targetAuth = $auth.target
                    if ((Get-Member -InputObject $targetAuth -Name "username" -MemberType Properties) -and 
                        (Get-Member -InputObject $targetAuth -Name "password" -MemberType Properties)) {
                        
                        $securePassword = ConvertTo-SecureString $targetAuth.password -AsPlainText -Force
                        $targetCredential = New-Object System.Management.Automation.PSCredential($targetAuth.username, $securePassword)
                        
                        $params.Add("TargetCredential", $targetCredential)
                        Write-Log "Using SQL authentication for target from config file" -Level INFO
                    } else {
                        Write-Log "Target SQL authentication specified but username or password missing" -Level WARNING
                    }
                }
                
                # For backward compatibility - check for common credentials
                if ((-not (Get-Member -InputObject $auth -Name "source" -MemberType Properties)) -and
                    (-not (Get-Member -InputObject $auth -Name "target" -MemberType Properties)) -and
                    (Get-Member -InputObject $auth -Name "username" -MemberType Properties) -and 
                    (Get-Member -InputObject $auth -Name "password" -MemberType Properties)) {
                    
                    $securePassword = ConvertTo-SecureString $auth.password -AsPlainText -Force
                    $credential = New-Object System.Management.Automation.PSCredential($auth.username, $securePassword)
                    
                    $params.Add("SourceCredential", $credential)
                    $params.Add("TargetCredential", $credential)
                    
                    Write-Log "Using common SQL authentication for source and target from config file" -Level INFO
                }
            } elseif ($auth.type -eq "windows") {
                Write-Log "Using Windows authentication from config file" -Level INFO
                # Windows authentication doesn't require credentials
            } else {
                Write-Log "Unknown authentication type: $($auth.type)" -Level WARNING
            }
        }
    }
    
    return $params
}

# Validate Required Parameters
function Validate-Parameters {
    param(
        [Parameter(Mandatory=$true)]
        [string]$SourceServer,
        
        [Parameter(Mandatory=$true)]
        [string]$SourceDB,
        
        [Parameter(Mandatory=$true)]
        [string]$TargetServer,
        
        [Parameter(Mandatory=$true)]
        [string]$TargetDB
    )
    
    $missingParams = @()
    
    if (-not $SourceServer) { $missingParams += "SourceServer" }
    if (-not $SourceDB) { $missingParams += "SourceDB" }
    if (-not $TargetServer) { $missingParams += "TargetServer" }
    if (-not $TargetDB) { $missingParams += "TargetDB" }
    
    if ($missingParams.Count -gt 0) {
        Write-Log "Missing required parameters: $($missingParams -join ', ')" -Level ERROR
        throw "Missing required parameters: $($missingParams -join ', ')"
    }
    
    return $true
}

# Load environment variables from .env file if it exists
function Load-EnvFile {
    param (
        [string]$EnvFilePath = ".env"
    )
    
    if (Test-Path $EnvFilePath) {
        # Check if Write-Log function is available and log file is initialized
        $logInitialized = $false
        try {
            if ($global:LogFilePath -ne $null) {
                $logInitialized = $true
                Write-Log "Loading environment variables from $EnvFilePath" -Level INFO
            } else {
                Write-Host "Loading environment variables from $EnvFilePath"
            }
        } catch {
            Write-Host "Loading environment variables from $EnvFilePath"
        }
        
        $envContent = Get-Content $EnvFilePath -ErrorAction SilentlyContinue
        
        foreach ($line in $envContent) {
            if ($line -match '^\s*([^#][^=]+)=(.*)$') {
                $key = $matches[1].Trim()
                $value = $matches[2].Trim()

                # Remove quotes if present
                if ($value -match '^["''](.*)[''"]$') {
                    $value = $matches[1]
                }

                # Set as environment variable
                [Environment]::SetEnvironmentVariable($key, $value, [System.EnvironmentVariableTarget]::Process)
                
                if ($logInitialized) {
                    Write-Log "Set environment variable: $($key)" -Level VERBOSE
                } else {
                    Write-Verbose "Set environment variable: $($key)"
                }
            }
        }

        return $true
    } else {
        if ($global:LogFilePath -ne $null) {
            Write-Log "Environment file not found: $EnvFilePath" -Level WARNING
        } else {
            Write-Warning "Environment file not found: $EnvFilePath"
        }
        return $false
    }
}

# Create SQL credential from environment variables
function Get-SqlCredentialFromEnv {
    # Check if SQL credentials are provided in environment variables
    $sqlUser = [Environment]::GetEnvironmentVariable("sql_user")
    $sqlPassword = [Environment]::GetEnvironmentVariable("sql_password")
    
    if (-not [string]::IsNullOrEmpty($sqlUser) -and -not [string]::IsNullOrEmpty($sqlPassword)) {
        # Check if Write-Log function is available and log file is initialized
        if ($global:LogFilePath -ne $null) {
            Write-Log "Creating SQL credential for user: $sqlUser" -Level INFO
        } else {
            Write-Host "Creating SQL credential for user: $sqlUser"
        }
        
        $securePassword = ConvertTo-SecureString $sqlPassword -AsPlainText -Force
        return New-Object System.Management.Automation.PSCredential($sqlUser, $securePassword)
    }
    
    return $null
}

# Export module members
Export-ModuleMember -Function Load-Configuration, Convert-ConfigToParameters, Validate-Parameters, Load-EnvFile, Get-SqlCredentialFromEnv
