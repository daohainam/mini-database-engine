using System.Reflection;

namespace MiniDatabaseEngine.Orm;

public sealed class EntityMapper<T> where T : class, new()
{
    private static readonly NullabilityInfoContext NullabilityContext = new();
    private static readonly EntityMetadata Metadata = BuildMetadata();

    public string TableName => Metadata.TableName;
    public string PrimaryKeyColumn => Metadata.PrimaryKeyColumn;
    public IReadOnlyList<ColumnDefinition> Columns => Metadata.Columns;

    public TableSchema CreateSchema() => new(Metadata.TableName, [.. Metadata.Columns], Metadata.PrimaryKeyColumn);

    public object GetPrimaryKeyValue(T entity)
    {
        ArgumentNullException.ThrowIfNull(entity);

        if (Metadata.PrimaryKeyProperty == null)
            throw new InvalidOperationException($"Entity '{typeof(T).Name}' does not define a primary key property.");

        var value = Metadata.PrimaryKeyProperty.GetValue(entity)
            ?? throw new InvalidOperationException($"Primary key property '{Metadata.PrimaryKeyProperty.Name}' cannot be null.");

        return value;
    }

    public DataRow ToDataRow(TableSchema schema, T entity)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(entity);

        var values = new object?[Metadata.PropertyMappings.Count];
        for (int i = 0; i < Metadata.PropertyMappings.Count; i++)
        {
            values[i] = Metadata.PropertyMappings[i].Property.GetValue(entity);
        }

        return new DataRow(schema, values);
    }

    public T FromDataRow(DataRow row)
    {
        ArgumentNullException.ThrowIfNull(row);

        var entity = new T();
        foreach (var mapping in Metadata.PropertyMappings)
        {
            var value = row[mapping.ColumnName];
            mapping.Property.SetValue(entity, ConvertToPropertyType(value, mapping.Property.PropertyType, mapping.ColumnName));
        }

        return entity;
    }

    private static EntityMetadata BuildMetadata()
    {
        var entityType = typeof(T);
        var tableName = entityType.GetCustomAttribute<TableNameAttribute>()?.Name ?? entityType.Name;

        var properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite && p.GetIndexParameters().Length == 0)
            .Where(p => p.GetCustomAttribute<NotMappedAttribute>() == null)
            .ToList();

        if (properties.Count == 0)
            throw new InvalidOperationException($"Entity '{entityType.Name}' must have at least one mappable public get/set property.");

        var mappings = new List<PropertyMapping>(properties.Count);
        PropertyInfo? primaryKeyProperty = null;

        foreach (var property in properties)
        {
            var columnName = property.GetCustomAttribute<ColumnNameAttribute>()?.Name ?? property.Name;
            var dataType = ResolveDataType(property.PropertyType);
            var isNullable = ResolveNullable(property);
            var maxLength = property.GetCustomAttribute<MaxLengthAttribute>()?.Length ?? 255;

            mappings.Add(new PropertyMapping(property, columnName));

            if (property.GetCustomAttribute<PrimaryKeyAttribute>() != null)
            {
                if (primaryKeyProperty != null)
                {
                    throw new InvalidOperationException(
                        $"Entity '{entityType.Name}' has multiple [PrimaryKey] properties: '{primaryKeyProperty.Name}' and '{property.Name}'.");
                }

                primaryKeyProperty = property;
            }

            var column = new ColumnDefinition(columnName, dataType, isNullable, maxLength);
            mappings[^1].ColumnDefinition = column;
        }

        var duplicateColumn = mappings.GroupBy(m => m.ColumnName, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault(g => g.Count() > 1);
        if (duplicateColumn != null)
            throw new InvalidOperationException($"Entity '{entityType.Name}' has duplicate mapped column name '{duplicateColumn.Key}'.");

        var columns = mappings.Select(m => m.ColumnDefinition!).ToList();
        var primaryKeyColumn = primaryKeyProperty == null
            ? string.Empty
            : mappings.First(m => m.Property == primaryKeyProperty).ColumnName;

        return new EntityMetadata(tableName, primaryKeyColumn, columns, mappings, primaryKeyProperty);
    }

    private static bool ResolveNullable(PropertyInfo property)
    {
        var nullableAttribute = property.GetCustomAttribute<NullableAttribute>();
        if (nullableAttribute != null)
            return nullableAttribute.IsNullable;

        var propertyType = property.PropertyType;
        if (Nullable.GetUnderlyingType(propertyType) != null)
            return true;

        if (!propertyType.IsValueType)
        {
            var nullability = NullabilityContext.Create(property);
            return nullability.WriteState != NullabilityState.NotNull;
        }

        return false;
    }

    private static DataType ResolveDataType(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;

        return Type.GetTypeCode(underlying) switch
        {
            TypeCode.Byte => DataType.Byte,
            TypeCode.SByte => DataType.SByte,
            TypeCode.Int16 => DataType.Short,
            TypeCode.UInt16 => DataType.UShort,
            TypeCode.Int32 => DataType.Int,
            TypeCode.UInt32 => DataType.UInt,
            TypeCode.Int64 => DataType.Long,
            TypeCode.UInt64 => DataType.ULong,
            TypeCode.Boolean => DataType.Bool,
            TypeCode.Char => DataType.Char,
            TypeCode.String => DataType.String,
            TypeCode.Single => DataType.Float,
            TypeCode.Double => DataType.Double,
            TypeCode.Decimal => DataType.Decimal,
            TypeCode.DateTime => DataType.DateTime,
            _ => throw new NotSupportedException($"Property type '{underlying.Name}' is not supported by MiniDatabaseEngine data types.")
        };
    }

    private static object? ConvertToPropertyType(object? value, Type propertyType, string columnName)
    {
        if (value == null)
        {
            if (Nullable.GetUnderlyingType(propertyType) != null || !propertyType.IsValueType)
                return null;

            throw new InvalidOperationException($"Column '{columnName}' returned null for non-nullable property type '{propertyType.Name}'.");
        }

        var targetType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
        if (targetType.IsInstanceOfType(value))
            return value;

        return Convert.ChangeType(value, targetType);
    }

    private sealed class EntityMetadata(
        string tableName,
        string primaryKeyColumn,
        IReadOnlyList<ColumnDefinition> columns,
        IReadOnlyList<PropertyMapping> propertyMappings,
        PropertyInfo? primaryKeyProperty)
    {
        public string TableName { get; } = tableName;
        public string PrimaryKeyColumn { get; } = primaryKeyColumn;
        public IReadOnlyList<ColumnDefinition> Columns { get; } = columns;
        public IReadOnlyList<PropertyMapping> PropertyMappings { get; } = propertyMappings;
        public PropertyInfo? PrimaryKeyProperty { get; } = primaryKeyProperty;
    }

    private sealed class PropertyMapping(PropertyInfo property, string columnName)
    {
        public PropertyInfo Property { get; } = property;
        public string ColumnName { get; } = columnName;
        public ColumnDefinition? ColumnDefinition { get; set; }
    }
}
