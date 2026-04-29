namespace MiniDatabaseEngine.Orm;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class ColumnNameAttribute(string name) : Attribute
{
    public string Name { get; } = string.IsNullOrWhiteSpace(name)
        ? throw new ArgumentException("Column name cannot be null or whitespace.", nameof(name))
        : name;
}
