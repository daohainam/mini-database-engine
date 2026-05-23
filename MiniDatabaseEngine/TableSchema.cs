namespace MiniDatabaseEngine;

/// <summary>
/// Defines the schema for a table
/// </summary>
public class TableSchema
{
    public string TableName { get; }
    public IReadOnlyList<ColumnDefinition> Columns { get; }
    public string PrimaryKeyColumn { get; }
    private readonly Dictionary<string, int> _columnIndexMap;
    
    public TableSchema(string tableName, List<ColumnDefinition> columns, string primaryKeyColumn)
    {
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        if (columns == null) throw new ArgumentNullException(nameof(columns));
        Columns = columns.AsReadOnly();
        PrimaryKeyColumn = primaryKeyColumn;
        
        if (columns.Count == 0)
            throw new ArgumentException("Table must have at least one column", nameof(columns));
            
        if (!string.IsNullOrEmpty(primaryKeyColumn) && !columns.Any(c => c.Name == primaryKeyColumn))
            throw new ArgumentException($"Primary key column '{primaryKeyColumn}' not found in columns", nameof(primaryKeyColumn));

        _columnIndexMap = new Dictionary<string, int>(columns.Count, StringComparer.Ordinal);
        for (int i = 0; i < columns.Count; i++)
        {
            _columnIndexMap[columns[i].Name] = i;
        }
    }
    
    public int GetColumnIndex(string columnName)
    {
        return _columnIndexMap.TryGetValue(columnName, out var index) ? index : -1;
    }
}
