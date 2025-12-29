namespace MiniDatabaseEngine;

/// <summary>
/// Defines the schema for a table
/// </summary>
public class TableSchema
{
    public string TableName { get; set; }
    public List<ColumnDefinition> Columns { get; set; }
    public string PrimaryKeyColumn { get; set; }
    
    public TableSchema(string tableName, List<ColumnDefinition> columns, string primaryKeyColumn)
    {
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        Columns = columns ?? throw new ArgumentNullException(nameof(columns));
        PrimaryKeyColumn = primaryKeyColumn;
        
        if (columns.Count == 0)
            throw new ArgumentException("Table must have at least one column", nameof(columns));
            
        if (!string.IsNullOrEmpty(primaryKeyColumn) && !columns.Any(c => c.Name == primaryKeyColumn))
            throw new ArgumentException($"Primary key column '{primaryKeyColumn}' not found in columns", nameof(primaryKeyColumn));
    }
    
    public int GetColumnIndex(string columnName)
    {
        for (int i = 0; i < Columns.Count; i++)
        {
            if (Columns[i].Name == columnName)
                return i;
        }
        return -1;
    }
}
