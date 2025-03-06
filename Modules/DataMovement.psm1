# DataMovement.psm1

function Export-TablePartition {
    param (
        [Parameter(Mandatory = $true)]
        [string]$SourceServer,
        
        [Parameter(Mandatory = $true)]
        [string]$SourceDatabase,
        
        [Parameter(Mandatory = $true)]
        [string]$TableName,
        
        [Parameter(Mandatory = $false)]
        [bool]$IsPartitioned = $false,
        
        [Parameter(Mandatory = $false)]
        [string]$PartitionFunction = "",
        
        [Parameter(Mandatory = $false)]
        [string]$PartitionColumn = "",
        
        [Parameter(Mandatory = $true)]
        [int]$PartitionNumber,
        
        [Parameter(Mandatory = $true)]
        [string]$OutputPath,
        
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
    
    # Create output file path without using Join-Path to avoid path duplication
    if ($OutputPath.EndsWith('\') -or $OutputPath.EndsWith('/')) {
        $outputFile = "$OutputPath$($schema)_$($table)_p$PartitionNumber.dat"
    } else {
        $outputFile = "$OutputPath\$($schema)_$($table)_p$PartitionNumber.dat"
    }
    
    # Create query based on partitioning
    if ($IsPartitioned) {
        $query = 'SELECT * FROM [' + $schema + '].[' + $table + '] WHERE $partition.[' + $PartitionFunction + ']([' + $PartitionColumn + ']) = ' + $PartitionNumber
    } else {
        $query = 'SELECT * FROM [' + $schema + '].[' + $table + ']'
    }
    
    # Set authentication parameters
    $authParams = if ($Authentication -eq "windows") {
        "-T"
    } else {
        "-U `"$Username`" -P `"$Password`""
    }
    
    # Execute BCP export
    $bcpCommand = "bcp `"$query`" queryout `"$outputFile`" -S `"$SourceServer`" -d `"$SourceDatabase`" $authParams -n"
    Write-Verbose "Executing: $bcpCommand"
    
    $process = Start-Process -FilePath "bcp" -ArgumentList $bcpCommand.Substring(4) -NoNewWindow -Wait -PassThru
    
    if ($process.ExitCode -ne 0) {
        throw "BCP export failed with exit code $($process.ExitCode)"
    }
    
    return $outputFile
}

function Import-TablePartition {
    param (
        [Parameter(Mandatory = $true)]
        [string]$DestServer,
        
        [Parameter(Mandatory = $true)]
        [string]$DestDatabase,
        
        [Parameter(Mandatory = $true)]
        [string]$TableName,
        
        [Parameter(Mandatory = $true)]
        [string]$InputFile,
        
        [Parameter(Mandatory = $true)]
        [int]$BatchSize,
        
        [Parameter(Mandatory = $false)]
        [string]$Authentication = "windows",
        
        [Parameter(Mandatory = $false)]
        [string]$Username = "",
        
        [Parameter(Mandatory = $false)]
        [string]$Password = ""
    )
    
    # Set authentication parameters
    $authParams = if ($Authentication -eq "windows") {
        "-T"
    } else {
        "-U `"$Username`" -P `"$Password`""
    }
    
    # Execute BCP import
    $bcpCommand = "bcp `"$TableName`" in `"$InputFile`" -S `"$DestServer`" -d `"$DestDatabase`" $authParams -n -b $BatchSize"
    Write-Verbose "Executing: $bcpCommand"
    
    $process = Start-Process -FilePath "bcp" -ArgumentList $bcpCommand.Substring(4) -NoNewWindow -Wait -PassThru
    
    if ($process.ExitCode -ne 0) {
        throw "BCP import failed with exit code $($process.ExitCode)"
    }
    
    # Delete temporary file after successful import
    if (Test-Path -Path $InputFile) {
        Remove-Item -Path $InputFile -Force
        Write-Verbose "Temporary file deleted: $InputFile"
    }
}

Export-ModuleMember -Function Export-TablePartition, Import-TablePartition
