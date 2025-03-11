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
        return $config
    } catch {
        Write-Log "Error loading configuration: $($_.Exception.Message)" -Level ERROR
        throw "Error loading configuration: $($_.Exception.Message)"
    }
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
Export-ModuleMember -Function Load-Configuration, Validate-Parameters, Load-EnvFile, Get-SqlCredentialFromEnv
