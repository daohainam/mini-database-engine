namespace MiniDatabaseEngine.Orm;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class TableNameAttribute(string name) : Attribute
{
    public string Name { get; } = string.IsNullOrWhiteSpace(name)
        ? throw new ArgumentException("Table name cannot be null or whitespace.", nameof(name))
        : name;
}
