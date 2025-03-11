# Database Operations Module for SmartBCP
# Contains functions for database operations

# Get Tables from Database
function Get-Tables {
    param (
        [string]$Server,
        [string]$Database,
        [string]$SchemaFilter = "%",
        [string]$TableFilter = "%",
        [System.Management.Automation.PSCredential]$Credential
    )

    $query = @"
    SELECT 
        t.TABLE_SCHEMA,
        t.TABLE_NAME,
        t.TABLE_TYPE,
        OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME) AS TableObjectID,
        (SELECT COUNT(*) FROM sys.indexes i 
         WHERE i.object_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME) 
         AND i.is_primary_key = 1) AS HasPrimaryKey,
        OBJECTPROPERTY(OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME), 'TableHasIdentity') AS HasIdentity
    FROM 
        INFORMATION_SCHEMA.TABLES t
    WHERE 
        t.TABLE_TYPE = 'BASE TABLE'
        AND t.TABLE_SCHEMA != 'sys'
        AND t.TABLE_CATALOG = DB_NAME()
        AND t.TABLE_SCHEMA LIKE '$SchemaFilter'
        AND t.TABLE_NAME LIKE '$TableFilter'
    ORDER BY 
        t.TABLE_SCHEMA, t.TABLE_NAME;
"@

    try {
        if ($Credential) {
            $tables = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query -Credential $Credential -TrustServerCertificate
            Write-Log "Using SQL authentication for database connection" -Level INFO
        } else {
            $tables = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query -TrustServerCertificate
            Write-Log "Using Windows authentication for database connection" -Level INFO
        }
        
        Write-Log "Retrieved $($tables.Count) tables from $Database on $Server" -Level INFO
        return $tables
    } catch {
        Write-Log ("Error fetching table list from {0} on {1}: {2}" -f $Database, $Server, $_.Exception.Message) -Level ERROR
        throw $_
    }
}

# Truncate Table
function Truncate-Table {
    param (
        [string]$Server,
        [string]$Database,
        [string]$Schema,
        [string]$Table,
        [bool]$ResetIdentity = $true,
        [System.Management.Automation.PSCredential]$Credential
    )

    $resetIdentityClause = ""
    if ($ResetIdentity) {
        $resetIdentityClause = "DBCC CHECKIDENT ('[$Schema].[$Table]', RESEED, 0); -- Reset identity if present"
    }

    $query = @"
    SET NOCOUNT ON;
    BEGIN TRY
        TRUNCATE TABLE [$Schema].[$Table];
        $resetIdentityClause
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
"@

    try {
        if ($Credential) {
            Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query -Credential $Credential -TrustServerCertificate
        } else {
            # Use Windows authentication
            Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query -TrustServerCertificate
            Write-Log "Using Windows authentication for database connection" -Level INFO
        }
        
        Write-Log "Truncated table [$Schema].[$Table] in $Database on $Server" -Level DEBUG
        return $true
    } catch {
        Write-Log ("Error truncating table {0}: {1}" -f "[$Schema].[$Table]", $_.Exception.Message) -Level WARNING
        return $false
    }
}

# Test database connection
function Test-DatabaseConnection {
    param (
        [Parameter(Mandatory=$true)]
        [string]$Server,
        
        [Parameter(Mandatory=$true)]
        [string]$Database,
        
        [Parameter(Mandatory=$false)]
        [System.Management.Automation.PSCredential]$Credential
    )
    
    try {
        $query = "SELECT @@VERSION AS Version, DB_NAME() AS DatabaseName, GETDATE() AS CurrentTime"
        
        if ($Credential) {
            $result = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query -Credential $Credential -TrustServerCertificate -ErrorAction Stop
        } else {
            $result = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query -TrustServerCertificate -ErrorAction Stop
        }
        
        return @{
            Success = $true
            Version = $result.Version
            DatabaseName = $result.DatabaseName
            CurrentTime = $result.CurrentTime
        }
    } catch {
        return @{
            Success = $false
            Error = $_.Exception.Message
        }
    }
}

# Get row count for a table
function Get-TableRowCount {
    param (
        [string]$Server,
        [string]$Database,
        [string]$Schema,
        [string]$Table,
        [System.Management.Automation.PSCredential]$Credential
    )

    $query = @"
    SELECT COUNT(*) AS RowCount FROM [$Schema].[$Table] WITH (NOLOCK);
"@

    try {
        if ($Credential) {
            $result = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query -Credential $Credential -TrustServerCertificate
        } else {
            $result = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query -TrustServerCertificate
        }
        
        return $result.RowCount
    } catch {
        Write-Log ("Error getting row count for table {0}: {1}" -f "[$Schema].[$Table]", $_.Exception.Message) -Level WARNING
        return -1
    }
}

# Export module members
Export-ModuleMember -Function Get-Tables, Truncate-Table, Test-DatabaseConnection, Get-TableRowCount
