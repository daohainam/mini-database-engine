namespace MiniDatabaseEngine.Orm;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class MaxLengthAttribute(int length) : Attribute
{
    public int Length { get; } = length > 0
        ? length
        : throw new ArgumentOutOfRangeException(nameof(length), "Max length must be greater than zero.");
}
