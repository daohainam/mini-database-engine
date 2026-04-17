namespace MiniDatabaseEngine;

/// <summary>
/// Defines a column in a table
/// </summary>
public class ColumnDefinition
{
    public string Name { get; }
    public DataType DataType { get; }
    public bool IsNullable { get; }
    public int MaxLength { get; } // For string types
    
    public ColumnDefinition(string name, DataType dataType, bool isNullable = true, int maxLength = 255)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        DataType = dataType;
        IsNullable = isNullable;
        MaxLength = maxLength;
    }
}
