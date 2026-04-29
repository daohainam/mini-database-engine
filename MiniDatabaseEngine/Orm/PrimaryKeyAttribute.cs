namespace MiniDatabaseEngine.Orm;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class PrimaryKeyAttribute : Attribute;
