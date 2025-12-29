namespace MiniDatabaseEngine;

/// <summary>
/// Defines a column in a table
/// </summary>
public class ColumnDefinition
{
    public string Name { get; set; }
    public DataType DataType { get; set; }
    public bool IsNullable { get; set; }
    public int MaxLength { get; set; } // For string types
    
    public ColumnDefinition(string name, DataType dataType, bool isNullable = true, int maxLength = 255)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        DataType = dataType;
        IsNullable = isNullable;
        MaxLength = maxLength;
    }
}
