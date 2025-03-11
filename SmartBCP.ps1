<#
.SYNOPSIS
    SmartBCP - A PowerShell utility for efficient SQL Server database migration using native BCP
.DESCRIPTION
    SmartBCP emulates SmartBulkCopy functionality using only native SQL Server tools.
    It performs parallel data migration between SQL Server databases with identical schema,
    handling foreign key constraints, optimizing performance, and managing temporary resources.
.PARAMETER ConfigFile
    Path to a JSON configuration file containing migration settings
.PARAMETER SourceServer
    Source SQL Server instance name
.PARAMETER SourceDB
    Source database name
.PARAMETER TargetServer
    Target SQL Server instance name
.PARAMETER TargetDB
    Target database name
.PARAMETER ParallelTasks
    Number of tables to migrate in parallel (default: 4)
.PARAMETER BatchSize
    Number of rows per batch in BCP operations (default: 10000)
.PARAMETER TempPath
    Path for temporary files (default: script directory)
.PARAMETER Format
    BCP format to use: native, char, or widechar (default: native)
.PARAMETER ManageForeignKeys
    Whether to handle foreign key constraints (default: true)
.PARAMETER LogPath
    Directory to store log files (default: script directory)
.PARAMETER SourceCredential
    SQL credentials for source database connection
.PARAMETER TargetCredential
    SQL credentials for target database connection
.EXAMPLE
    .\SmartBCP.ps1 -SourceServer "SourceSQLServer" -SourceDB "AdventureWorks" -TargetServer "TargetSQLServer" -TargetDB "AdventureWorks_Copy" -ParallelTasks 8
.EXAMPLE
    .\SmartBCP.ps1 -ConfigFile "migration-config.json"
.NOTES
    Author: SmartBCP Team
    Date: 2025-03-11
    Version: 1.0
#>

#Requires -Version 5.1
#Requires -Modules SqlServer

[CmdletBinding(DefaultParameterSetName="Direct")]
param(
    [Parameter(Mandatory=$true, ParameterSetName="ConfigFile")]
    [string]$ConfigFile,
    
    [Parameter(Mandatory=$true, ParameterSetName="Direct")]
    [string]$SourceServer,
    
    [Parameter(Mandatory=$true, ParameterSetName="Direct")]
    [string]$SourceDB,
    
    [Parameter(Mandatory=$true, ParameterSetName="Direct")]
    [string]$TargetServer,
    
    [Parameter(Mandatory=$true, ParameterSetName="Direct")]
    [string]$TargetDB,
    
    [Parameter(Mandatory=$false, ParameterSetName="Direct")]
    [string[]]$IncludeSchemas,
    
    [Parameter(Mandatory=$false, ParameterSetName="Direct")]
    [string[]]$ExcludeSchemas,
    
    [Parameter(Mandatory=$false, ParameterSetName="Direct")]
    [string[]]$IncludeTables,
    
    [Parameter(Mandatory=$false, ParameterSetName="Direct")]
    [string[]]$ExcludeTables,
    
    [Parameter(Mandatory=$false, ParameterSetName="Direct")]
    [switch]$TruncateTargetTables,
    
    [Parameter(Mandatory=$false, ParameterSetName="Direct")]
    [switch]$ManageForeignKeys,
    
    [Parameter(Mandatory=$false, ParameterSetName="Direct")]
    [int]$ParallelTasks = 4,
    
    [Parameter(Mandatory=$false, ParameterSetName="Direct")]
    [ValidateSet("native", "char", "widechar")]
    [string]$BCPFormat = "native",

    [Parameter(Mandatory=$false, ParameterSetName="Direct")]
    [string]$TempPath = ".\Temp",
    
    [Parameter(Mandatory=$false, ParameterSetName="Direct")]
    [System.Management.Automation.PSCredential]$SourceCredential,
    
    [Parameter(Mandatory=$false, ParameterSetName="Direct")]
    [System.Management.Automation.PSCredential]$TargetCredential
)

# Set strict mode and error handling
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Initialize global variables
$script:startTime = Get-Date
$script:scriptName = $MyInvocation.MyCommand.Name

# Import modules
$modulePath = Join-Path $PSScriptRoot "Modules"
Import-Module "$modulePath\Logging.psm1" -Force
Import-Module "$modulePath\Configuration.psm1" -Force
Import-Module "$modulePath\DatabaseOperations.psm1" -Force
Import-Module "$modulePath\ForeignKeyManagement.psm1" -Force
Import-Module "$modulePath\BCPOperations.psm1" -Force
Import-Module "$modulePath\ParallelProcessing.psm1" -Force
Import-Module "$modulePath\Core.psm1" -Force

# Load environment variables from .env file
Load-EnvFile -EnvFilePath ".env"

# Check if SQL credentials should be used from environment variables
if (-not $PSBoundParameters.ContainsKey("SourceCredential") -or -not $PSBoundParameters.ContainsKey("TargetCredential")) {
    $envCredential = Get-SqlCredentialFromEnv
    
    if ($envCredential -ne $null) {
        if (-not $PSBoundParameters.ContainsKey("SourceCredential")) {
            Write-Host "Using SQL credentials from environment for source connection"
            $PSBoundParameters["SourceCredential"] = $envCredential
        }
        
        if (-not $PSBoundParameters.ContainsKey("TargetCredential")) {
            Write-Host "Using SQL credentials from environment for target connection"
            $PSBoundParameters["TargetCredential"] = $envCredential
        }
    }
}

# Handle configuration file if provided
if ($PSCmdlet.ParameterSetName -eq "ConfigFile") {
    Write-Host "Loading configuration from $ConfigFile"
    $config = Load-Configuration -ConfigFilePath $ConfigFile
    $configParams = Convert-ConfigToParameters -Config $config
    
    # Call the main execution function with parameters from config file
    Start-SmartBCP @configParams
} else {
    # Call the main execution function with direct parameters
    Start-SmartBCP @PSBoundParameters
}
