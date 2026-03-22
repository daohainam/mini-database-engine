namespace MiniDatabaseEngine;

/// <summary>
/// Represents a row of data in a table
/// </summary>
public class DataRow
{
    private readonly object?[] _values;
    private readonly TableSchema _schema;
    
    public DataRow(TableSchema schema)
    {
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _values = new object?[schema.Columns.Count];
    }
    
    public DataRow(TableSchema schema, object?[] values) : this(schema)
    {
        if (values.Length != schema.Columns.Count)
            throw new ArgumentException($"Expected {schema.Columns.Count} values, got {values.Length}");
        
        Array.Copy(values, _values, values.Length);
    }
    
    public object? this[int index]
    {
        get
        {
            if (index < 0 || index >= _values.Length)
                throw new ArgumentOutOfRangeException(nameof(index), $"Column index {index} is out of range. Table has {_values.Length} columns.");
            return _values[index];
        }
        set
        {
            if (index < 0 || index >= _values.Length)
                throw new ArgumentOutOfRangeException(nameof(index), $"Column index {index} is out of range. Table has {_values.Length} columns.");
            _values[index] = value;
        }
    }
    
    public object? this[string columnName]
    {
        get
        {
            int index = _schema.GetColumnIndex(columnName);
            if (index == -1)
                throw new ArgumentException($"Column '{columnName}' not found");
            return _values[index];
        }
        set
        {
            int index = _schema.GetColumnIndex(columnName);
            if (index == -1)
                throw new ArgumentException($"Column '{columnName}' not found");
            _values[index] = value;
        }
    }
    
    public object?[] GetValues() => (object?[])_values.Clone();
    
    public T? GetValue<T>(string columnName)
    {
        var value = this[columnName];
        if (value == null)
            return default;
        
        // Validate type compatibility before casting
        if (value is T typedValue)
            return typedValue;
        
        throw new InvalidCastException(
            $"Cannot cast value of type '{value.GetType().Name}' to '{typeof(T).Name}' for column '{columnName}'");
    }
}
