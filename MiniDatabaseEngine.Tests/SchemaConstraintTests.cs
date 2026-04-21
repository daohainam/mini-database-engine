using Xunit;

namespace MiniDatabaseEngine.Tests;

public class SchemaConstraintTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly Database _database;

    public SchemaConstraintTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"test_constraints_{Guid.NewGuid()}.mde");
        _database = new Database(_testDbPath);
    }

    [Fact]
    public void Insert_Rejects_Null_For_NonNullable_Column()
    {
        var table = _database.CreateTable("Users", new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String, false)
        }, "Id");

        var row = new DataRow(table.Schema);
        row["Id"] = 1;
        row["Name"] = null;

        Assert.Throws<InvalidOperationException>(() => _database.Insert("Users", row));
    }

    [Fact]
    public void Insert_Rejects_Type_Mismatch()
    {
        var table = _database.CreateTable("Users", new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Age", DataType.Int, false)
        }, "Id");

        var row = new DataRow(table.Schema);
        row["Id"] = 1;
        row["Age"] = "not-an-int";

        Assert.Throws<InvalidCastException>(() => _database.Insert("Users", row));
    }

    [Fact]
    public void Insert_Rejects_String_Exceeding_MaxLength()
    {
        var table = _database.CreateTable("Users", new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String, false, maxLength: 5)
        }, "Id");

        var row = new DataRow(table.Schema);
        row["Id"] = 1;
        row["Name"] = "TooLongName";

        Assert.Throws<InvalidOperationException>(() => _database.Insert("Users", row));
    }

    [Fact]
    public void Insert_Rejects_Duplicate_Primary_Key()
    {
        var table = _database.CreateTable("Users", new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String, false)
        }, "Id");

        var first = new DataRow(table.Schema);
        first["Id"] = 1;
        first["Name"] = "Alice";
        _database.Insert("Users", first);

        var duplicate = new DataRow(table.Schema);
        duplicate["Id"] = 1;
        duplicate["Name"] = "Alice Duplicate";

        Assert.Throws<InvalidOperationException>(() => _database.Insert("Users", duplicate));
    }

    [Fact]
    public void Transaction_Insert_Rejects_Duplicate_Primary_Key_In_Same_Transaction()
    {
        var table = _database.CreateTable("Users", new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String, false)
        }, "Id");

        using var txn = _database.BeginTransaction();
        var first = new DataRow(table.Schema);
        first["Id"] = 10;
        first["Name"] = "Alice";
        _database.Insert("Users", first, txn);

        var duplicate = new DataRow(table.Schema);
        duplicate["Id"] = 10;
        duplicate["Name"] = "Alice Duplicate";

        Assert.Throws<InvalidOperationException>(() => _database.Insert("Users", duplicate, txn));
    }

    public void Dispose()
    {
        _database.Dispose();
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);

        var walPath = Path.ChangeExtension(_testDbPath, ".wal");
        if (File.Exists(walPath))
            File.Delete(walPath);
    }
}
