# Foreign Key Management Module for SmartBCP
# Contains functions for managing foreign key constraints

# Backup foreign key constraints
function Backup-ForeignKeyConstraints {
    param (
        [string]$Server,
        [string]$Database,
        [string]$BackupPath,
        [System.Management.Automation.PSCredential]$Credential
    )
    
    # Create backup directory if it doesn't exist
    if (-not (Test-Path $BackupPath)) {
        New-Item -Path $BackupPath -ItemType Directory -Force | Out-Null
        Write-Log "Created foreign key backup directory: $BackupPath" -Level INFO
    }
    
    # Query to get all foreign key constraints
    $query = @"
    SELECT 
        fk.name AS ForeignKeyName,
        OBJECT_SCHEMA_NAME(fk.parent_object_id) AS ParentSchema,
        OBJECT_NAME(fk.parent_object_id) AS ParentTable,
        COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS ParentColumn,
        OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ReferencedSchema,
        OBJECT_NAME(fk.referenced_object_id) AS ReferencedTable,
        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ReferencedColumn,
        fk.delete_referential_action AS DeleteAction,
        fk.update_referential_action AS UpdateAction
    FROM 
        sys.foreign_keys fk
    INNER JOIN 
        sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    ORDER BY 
        ParentSchema, ParentTable, ForeignKeyName;
"@
    
    try {
        # Execute query to get foreign key constraints
        if ($Credential) {
            $fkConstraints = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query -Credential $Credential -TrustServerCertificate
        } else {
            $fkConstraints = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query -TrustServerCertificate
        }
        
        # Group by foreign key name to handle multi-column foreign keys
        $groupedConstraints = $fkConstraints | Group-Object -Property ForeignKeyName
        
        # Create drop and create scripts
        $dropScriptPath = Join-Path $BackupPath "drop_foreign_keys.sql"
        $createScriptPath = Join-Path $BackupPath "create_foreign_keys.sql"
        
        $dropScript = ""
        $createScript = ""
        
        foreach ($group in $groupedConstraints) {
            $fk = $group.Group[0]
            $parentSchema = $fk.ParentSchema
            $parentTable = $fk.ParentTable
            $fkName = $fk.ForeignKeyName
            $referencedSchema = $fk.ReferencedSchema
            $referencedTable = $fk.ReferencedTable
            
            # Build drop statement
            $dropScript += "ALTER TABLE [$parentSchema].[$parentTable] DROP CONSTRAINT [$fkName];" + [Environment]::NewLine
            
            # Build create statement
            $createScript += "ALTER TABLE [$parentSchema].[$parentTable] ADD CONSTRAINT [$fkName] FOREIGN KEY ("
            
            # Add parent columns
            $parentColumns = $group.Group | ForEach-Object { "[$($_.ParentColumn)]" }
            $createScript += $parentColumns -join ", "
            
            $createScript += ") REFERENCES [$referencedSchema].[$referencedTable] ("
            
            # Add referenced columns
            $referencedColumns = $group.Group | ForEach-Object { "[$($_.ReferencedColumn)]" }
            $createScript += $referencedColumns -join ", "
            
            $createScript += ")"
            
            # Add delete action
            $deleteAction = switch ($fk.DeleteAction) {
                0 { "NO ACTION" }
                1 { "CASCADE" }
                2 { "SET NULL" }
                3 { "SET DEFAULT" }
                default { "NO ACTION" }
            }
            $createScript += " ON DELETE $deleteAction"
            
            # Add update action
            $updateAction = switch ($fk.UpdateAction) {
                0 { "NO ACTION" }
                1 { "CASCADE" }
                2 { "SET NULL" }
                3 { "SET DEFAULT" }
                default { "NO ACTION" }
            }
            $createScript += " ON UPDATE $updateAction"
            
            $createScript += ";" + [Environment]::NewLine
        }
        
        # Write scripts to files
        [System.IO.File]::WriteAllText($dropScriptPath, $dropScript)
        [System.IO.File]::WriteAllText($createScriptPath, $createScript)
        
        Write-Log "Created foreign key drop script: $dropScriptPath" -Level INFO
        Write-Log "Created foreign key create script: $createScriptPath" -Level INFO
        
        return @{
            Count = $groupedConstraints.Count
            DropPath = $dropScriptPath
            CreatePath = $createScriptPath
        }
    } catch {
        Write-Log "Error backing up foreign key constraints: $($_.Exception.Message)" -Level ERROR
        throw $_
    }
}

# Drop foreign key constraints
function Drop-ForeignKeyConstraints {
    param (
        [string]$Server,
        [string]$Database,
        [string]$DropScriptPath,
        [System.Management.Automation.PSCredential]$Credential
    )
    
    try {
        # If a script path is provided, use it
        if ($DropScriptPath -and (Test-Path $DropScriptPath)) {
            $dropScript = Get-Content $DropScriptPath -Raw
            
            # Check if the script is not empty
            if (-not [string]::IsNullOrWhiteSpace($dropScript)) {
                if ($Credential) {
                    Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $dropScript -Credential $Credential -TrustServerCertificate
                } else {
                    Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $dropScript -TrustServerCertificate
                }
                
                Write-Log "Dropped foreign key constraints using script: $DropScriptPath" -Level INFO
            } else {
                Write-Log "No foreign key constraints to drop (empty script)" -Level INFO
            }
        } else {
            # Otherwise, generate and execute drop statements
            $query = @"
            DECLARE @sql NVARCHAR(MAX) = N'';

            SELECT @sql += N'
            ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] 
            DROP CONSTRAINT [' + name + '];'
            FROM sys.foreign_keys;

            EXEC sp_executesql @sql;
"@
            
            if ($Credential) {
                Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query -Credential $Credential -TrustServerCertificate
            } else {
                Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query -TrustServerCertificate
            }
            
            Write-Log "Dropped all foreign key constraints in $Database on $Server" -Level INFO
        }
        
        return $true
    } catch {
        Write-Log "Error dropping foreign key constraints: $($_.Exception.Message)" -Level ERROR
        throw $_
    }
}

# Restore foreign key constraints
function Restore-ForeignKeyConstraints {
    param (
        [string]$Server,
        [string]$Database,
        [string]$CreateScriptPath,
        [System.Management.Automation.PSCredential]$Credential
    )
    
    try {
        if (-not (Test-Path $CreateScriptPath)) {
            Write-Log "Foreign key create script not found: $CreateScriptPath" -Level ERROR
            return $false
        }
        
        $createScript = Get-Content $CreateScriptPath -Raw
        
        # Check if the script is not empty
        if (-not [string]::IsNullOrWhiteSpace($createScript)) {
            if ($Credential) {
                Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $createScript -Credential $Credential -TrustServerCertificate
            } else {
                Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $createScript -TrustServerCertificate
            }
            
            Write-Log "Restored foreign key constraints using script: $CreateScriptPath" -Level INFO
        } else {
            Write-Log "No foreign key constraints to restore (empty script)" -Level INFO
        }
        return $true
    } catch {
        Write-Log "Error restoring foreign key constraints: $($_.Exception.Message)" -Level ERROR
        return $false
    }
}

# Export module members
Export-ModuleMember -Function Backup-ForeignKeyConstraints, Drop-ForeignKeyConstraints, Restore-ForeignKeyConstraints
