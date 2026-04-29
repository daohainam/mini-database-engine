using MiniDatabaseEngine.Orm;
using Xunit;

namespace MiniDatabaseEngine.Tests;

public sealed class OrmTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly Database _database;

    public OrmTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"orm_test_{Guid.NewGuid()}.mde");
        _database = new Database(_testDbPath);
    }

    [Fact]
    public void CreateTable_FromEntity_BuildsExpectedSchema()
    {
        var table = _database.CreateTable<SimpleUserEntity>();

        Assert.Equal("SimpleUserEntity", table.TableName);
        Assert.Equal("Id", table.Schema.PrimaryKeyColumn);
        Assert.Equal(3, table.Schema.Columns.Count);

        Assert.Equal("Id", table.Schema.Columns[0].Name);
        Assert.Equal(DataType.Int, table.Schema.Columns[0].DataType);
        Assert.False(table.Schema.Columns[0].IsNullable);

        Assert.Equal("Name", table.Schema.Columns[1].Name);
        Assert.Equal(DataType.String, table.Schema.Columns[1].DataType);
        Assert.False(table.Schema.Columns[1].IsNullable);

        Assert.Equal("Age", table.Schema.Columns[2].Name);
        Assert.Equal(DataType.Int, table.Schema.Columns[2].DataType);
        Assert.True(table.Schema.Columns[2].IsNullable);
    }

    [Fact]
    public void ObjectTable_Crud_WorksWithTypedEntities()
    {
        var users = _database.CreateTable<SimpleUserEntity>();

        users.Insert(new SimpleUserEntity { Id = 1, Name = "Alice", Age = 30 });
        users.Insert(new SimpleUserEntity { Id = 2, Name = "Bob", Age = null });

        var single = users.SelectByKey(1);
        Assert.NotNull(single);
        Assert.Equal("Alice", single!.Name);
        Assert.Equal(30, single.Age);

        var updateResult = users.Update(new SimpleUserEntity { Id = 1, Name = "Alice Updated", Age = 31 });
        Assert.True(updateResult);

        var updated = users.SelectByKey(1);
        Assert.NotNull(updated);
        Assert.Equal("Alice Updated", updated!.Name);
        Assert.Equal(31, updated.Age);

        var queryResult = _database.Query<SimpleUserEntity>()
            .Where(u => u.Age.HasValue && u.Age.Value > 20)
            .OrderBy(u => u.Name)
            .ToList();

        Assert.Single(queryResult);
        Assert.Equal("Alice Updated", queryResult[0].Name);

        var deleteResult = users.Delete(2);
        Assert.True(deleteResult);
        Assert.Null(users.SelectByKey(2));
        Assert.Single(users.SelectAll());
    }

    [Fact]
    public void Attributes_AreAppliedForTableColumnPrimaryKeyAndNotMapped()
    {
        var table = _database.CreateTable<AttributedPersonEntity>();

        Assert.Equal("people", table.TableName);
        Assert.Equal("person_id", table.Schema.PrimaryKeyColumn);
        Assert.Equal(2, table.Schema.Columns.Count);
        Assert.Equal("person_id", table.Schema.Columns[0].Name);
        Assert.Equal("full_name", table.Schema.Columns[1].Name);

        var inserted = new AttributedPersonEntity
        {
            Id = 7,
            Name = "Eve",
            LocalOnlyNote = "should-not-persist"
        };

        table.Insert(inserted);

        var fetched = table.SelectByKey(7);
        Assert.NotNull(fetched);
        Assert.Equal(7, fetched!.Id);
        Assert.Equal("Eve", fetched.Name);
        Assert.Null(fetched.LocalOnlyNote);
    }

    [Fact]
    public void RoundTrip_AllSupportedTypes_Works()
    {
        var table = _database.CreateTable<AllTypesEntity>();
        var expected = new AllTypesEntity
        {
            Id = 1,
            ByteVal = byte.MaxValue,
            SByteVal = sbyte.MinValue,
            ShortVal = short.MinValue,
            UShortVal = ushort.MaxValue,
            UIntVal = uint.MaxValue,
            LongVal = long.MinValue + 1,
            ULongVal = ulong.MaxValue,
            BoolVal = true,
            CharVal = 'Z',
            StringVal = "Hello ORM",
            FloatVal = 1.5f,
            DoubleVal = 9.25,
            DecimalVal = 12345.6789m,
            DateTimeVal = new DateTime(2025, 5, 1, 12, 30, 0, DateTimeKind.Utc)
        };

        table.Insert(expected);

        var actual = table.SelectByKey(1);
        Assert.NotNull(actual);

        Assert.Equal(expected.ByteVal, actual!.ByteVal);
        Assert.Equal(expected.SByteVal, actual.SByteVal);
        Assert.Equal(expected.ShortVal, actual.ShortVal);
        Assert.Equal(expected.UShortVal, actual.UShortVal);
        Assert.Equal(expected.UIntVal, actual.UIntVal);
        Assert.Equal(expected.LongVal, actual.LongVal);
        Assert.Equal(expected.ULongVal, actual.ULongVal);
        Assert.Equal(expected.BoolVal, actual.BoolVal);
        Assert.Equal(expected.CharVal, actual.CharVal);
        Assert.Equal(expected.StringVal, actual.StringVal);
        Assert.Equal(expected.FloatVal, actual.FloatVal);
        Assert.Equal(expected.DoubleVal, actual.DoubleVal);
        Assert.Equal(expected.DecimalVal, actual.DecimalVal);
        Assert.Equal(expected.DateTimeVal, actual.DateTimeVal);
    }

    [Fact]
    public void NullableAndMaxLengthConstraints_AreEnforced()
    {
        var table = _database.CreateTable<ConstrainedEntity>();

        Assert.Throws<InvalidOperationException>(() =>
            table.Insert(new ConstrainedEntity { Id = 1, RequiredName = null, Code = "OK" }));

        Assert.Throws<InvalidOperationException>(() =>
            table.Insert(new ConstrainedEntity { Id = 2, RequiredName = "abc", Code = "TOO-LONG" }));

        table.Insert(new ConstrainedEntity { Id = 3, RequiredName = "valid", Code = "12345" });
        var actual = table.SelectByKey(3);
        Assert.NotNull(actual);
        Assert.Equal("valid", actual!.RequiredName);
        Assert.Equal("12345", actual.Code);
    }

    public void Dispose()
    {
        _database.Dispose();
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
    }

    public sealed class SimpleUserEntity
    {
        [PrimaryKey]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int? Age { get; set; }
    }

    [TableName("people")]
    public sealed class AttributedPersonEntity
    {
        [PrimaryKey]
        [ColumnName("person_id")]
        public int Id { get; set; }

        [ColumnName("full_name")]
        public string Name { get; set; } = string.Empty;

        [NotMapped]
        public string? LocalOnlyNote { get; set; }
    }

    public sealed class AllTypesEntity
    {
        [PrimaryKey]
        public int Id { get; set; }
        public byte ByteVal { get; set; }
        public sbyte SByteVal { get; set; }
        public short ShortVal { get; set; }
        public ushort UShortVal { get; set; }
        public uint UIntVal { get; set; }
        public long LongVal { get; set; }
        public ulong ULongVal { get; set; }
        public bool BoolVal { get; set; }
        public char CharVal { get; set; }
        public string StringVal { get; set; } = string.Empty;
        public float FloatVal { get; set; }
        public double DoubleVal { get; set; }
        public decimal DecimalVal { get; set; }
        public DateTime DateTimeVal { get; set; }
    }

    public sealed class ConstrainedEntity
    {
        [PrimaryKey]
        public int Id { get; set; }

        [Nullable(false)]
        public string? RequiredName { get; set; }

        [MaxLength(5)]
        public string? Code { get; set; }
    }
}
