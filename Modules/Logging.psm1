# Logging Module for SmartBCP
# Contains functions for logging operations

# Global log file path
$global:LogFilePath = $null

# Initialize logging
function Initialize-Logging {
    param (
        [Parameter(Mandatory=$true)]
        [string]$LogDir,
        
        [Parameter(Mandatory=$false)]
        [string]$Timestamp = (Get-Date -Format "yyyyMMdd_HHmmss")
    )
    
    # Create log directory if it doesn't exist
    if (-not (Test-Path $LogDir)) {
        New-Item -Path $LogDir -ItemType Directory -Force | Out-Null
    }

    # Set up global log file path
    $global:LogFilePath = Join-Path $LogDir "SmartBCP_${Timestamp}.log"
    New-Item -Path $global:LogFilePath -ItemType File -Force | Out-Null
    Write-Verbose "Initialized log file: $global:LogFilePath"

    # Initialize log file with timestamp
    try {
        # Ensure the file is created and accessible
        $null = New-Item -Path $global:LogFilePath -ItemType File -Force
        # Write a header line to verify file is writable
        $headerLine = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] [INFO] SmartBCP Log File Initialized"
        [System.IO.File]::WriteAllText($global:LogFilePath, "$headerLine`r`n")
        Write-Verbose "Initialized log file: $global:LogFilePath"
        return $global:LogFilePath
    } catch {
        Write-Error "Failed to create log file: $($_.Exception.Message)"
        return $null
    }
}

function Write-Log {
    param(
        [Parameter(Mandatory=$true)]
        [string]$Message,
        
        [Parameter(Mandatory=$false)]
        [ValidateSet("INFO", "WARNING", "ERROR", "DEBUG", "VERBOSE")]
        [string]$Level = "INFO"
    )
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    
    # Write to console based on level
    switch ($Level) {
        "ERROR" { Write-Host $logMessage -ForegroundColor Red }
        "WARNING" { Write-Host $logMessage -ForegroundColor Yellow }
        "INFO" { Write-Host $logMessage -ForegroundColor White }
        "DEBUG" { if ($VerbosePreference -eq "Continue") { Write-Host $logMessage -ForegroundColor Gray } }
        "VERBOSE" { if ($VerbosePreference -eq "Continue") { Write-Host $logMessage -ForegroundColor DarkGray } }
    }
    
    # Write to log file with retry logic
    $maxRetries = 5
    $retryCount = 0
    $retryDelay = 100 # milliseconds
    
    while ($retryCount -lt $maxRetries) {
        try {
            # Verify log file exists, create if it doesn't
            if (-not (Test-Path -Path $global:LogFilePath)) {
                $logDir = [System.IO.Path]::GetDirectoryName($global:LogFilePath)
                if (-not (Test-Path -Path $logDir)) {
                    New-Item -Path $logDir -ItemType Directory -Force | Out-Null
                }
                $null = New-Item -Path $global:LogFilePath -ItemType File -Force
                Write-Verbose "Re-created missing log file: $global:LogFilePath"
            }
            
            # Use .NET methods for direct file access
            [System.IO.File]::AppendAllText($global:LogFilePath, "$logMessage`r`n")
            break
        }
        catch {
            $retryCount++
            if ($retryCount -eq $maxRetries) {
                Write-Warning "Failed to write to log file after $maxRetries attempts. Message: $Message. Error: $($_.Exception.Message)"
                break
            }
            Start-Sleep -Milliseconds $retryDelay
            # Increase delay with each retry
            $retryDelay *= 2
        }
    }
}

# Export module members
Export-ModuleMember -Function Initialize-Logging, Write-Log -Variable LogFilePath
