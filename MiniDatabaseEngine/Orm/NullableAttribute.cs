namespace MiniDatabaseEngine.Orm;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class NullableAttribute(bool isNullable = true) : Attribute
{
    public bool IsNullable { get; } = isNullable;
}
