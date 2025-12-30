using Xunit;

namespace MiniDatabaseEngine.Tests;

public class DatabaseTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly Database _database;
    
    public DatabaseTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.mde");
        _database = new Database(_testDbPath);
    }
    
    [Fact]
    public void CreateTable_Creates_New_Table()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String)
        };
        
        var table = _database.CreateTable("Users", columns, "Id");
        
        Assert.NotNull(table);
        Assert.Equal("Users", table.Schema.TableName);
        Assert.True(_database.TableExists("Users"));
    }
    
    [Fact]
    public void Insert_And_Query_Data()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String),
            new("Age", DataType.Int)
        };
        
        var table = _database.CreateTable("Users", columns, "Id");
        
        var row1 = new DataRow(table.Schema);
        row1["Id"] = 1;
        row1["Name"] = "Alice";
        row1["Age"] = 30;
        
        var row2 = new DataRow(table.Schema);
        row2["Id"] = 2;
        row2["Name"] = "Bob";
        row2["Age"] = 25;
        
        _database.Insert("Users", row1);
        _database.Insert("Users", row2);
        
        var result = table.SelectByKey(1);
        Assert.NotNull(result);
        Assert.Equal("Alice", result["Name"]);
        Assert.Equal(30, result["Age"]);
    }
    
    [Fact]
    public void Update_Modifies_Existing_Row()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String)
        };
        
        var table = _database.CreateTable("Users", columns, "Id");
        
        var row = new DataRow(table.Schema);
        row["Id"] = 1;
        row["Name"] = "Alice";
        _database.Insert("Users", row);
        
        var updatedRow = new DataRow(table.Schema);
        updatedRow["Id"] = 1;
        updatedRow["Name"] = "Alice Updated";
        
        var updated = _database.Update("Users", 1, updatedRow);
        Assert.True(updated);
        
        var result = table.SelectByKey(1);
        Assert.Equal("Alice Updated", result?["Name"]);
    }
    
    [Fact]
    public void Delete_Removes_Row()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String)
        };
        
        var table = _database.CreateTable("Users", columns, "Id");
        
        var row = new DataRow(table.Schema);
        row["Id"] = 1;
        row["Name"] = "Alice";
        _database.Insert("Users", row);
        
        var deleted = _database.Delete("Users", 1);
        Assert.True(deleted);
        
        var result = table.SelectByKey(1);
        Assert.Null(result);
    }
    
    [Fact]
    public void Query_With_Linq_Returns_All_Rows()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String),
            new("Age", DataType.Int)
        };
        
        var table = _database.CreateTable("Users", columns, "Id");
        
        for (int i = 1; i <= 5; i++)
        {
            var row = new DataRow(table.Schema);
            row["Id"] = i;
            row["Name"] = $"User{i}";
            row["Age"] = 20 + i;
            _database.Insert("Users", row);
        }
        
        var results = _database.Query("Users").ToList();
        Assert.Equal(5, results.Count);
    }
    
    [Fact]
    public void Supports_All_Data_Types()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("ByteVal", DataType.Byte),
            new("SByteVal", DataType.SByte),
            new("ShortVal", DataType.Short),
            new("UShortVal", DataType.UShort),
            new("UIntVal", DataType.UInt),
            new("LongVal", DataType.Long),
            new("ULongVal", DataType.ULong),
            new("BoolVal", DataType.Bool),
            new("CharVal", DataType.Char),
            new("StringVal", DataType.String),
            new("FloatVal", DataType.Float),
            new("DoubleVal", DataType.Double),
            new("DecimalVal", DataType.Decimal),
            new("DateTimeVal", DataType.DateTime)
        };
        
        var table = _database.CreateTable("AllTypes", columns, "Id");
        
        var row = new DataRow(table.Schema);
        row["Id"] = 1;
        row["ByteVal"] = (byte)255;
        row["SByteVal"] = (sbyte)-128;
        row["ShortVal"] = (short)-32768;
        row["UShortVal"] = (ushort)65535;
        row["UIntVal"] = (uint)4294967295;
        row["LongVal"] = 9223372036854775807L;
        row["ULongVal"] = 18446744073709551615UL;
        row["BoolVal"] = true;
        row["CharVal"] = 'A';
        row["StringVal"] = "Test String";
        row["FloatVal"] = 3.14f;
        row["DoubleVal"] = 3.141592653589793;
        row["DecimalVal"] = 1234567890.123456789m;
        row["DateTimeVal"] = new DateTime(2023, 12, 25, 10, 30, 0);
        
        _database.Insert("AllTypes", row);
        
        var result = table.SelectByKey(1);
        Assert.NotNull(result);
        Assert.Equal((byte)255, result["ByteVal"]);
        Assert.Equal((sbyte)-128, result["SByteVal"]);
        Assert.Equal((short)-32768, result["ShortVal"]);
        Assert.Equal((ushort)65535, result["UShortVal"]);
        Assert.Equal((uint)4294967295, result["UIntVal"]);
        Assert.Equal(9223372036854775807L, result["LongVal"]);
        Assert.Equal(18446744073709551615UL, result["ULongVal"]);
        Assert.Equal(true, result["BoolVal"]);
        Assert.Equal('A', result["CharVal"]);
        Assert.Equal("Test String", result["StringVal"]);
        Assert.Equal(3.14f, result["FloatVal"]);
        Assert.Equal(3.141592653589793, result["DoubleVal"]);
        Assert.Equal(1234567890.123456789m, result["DecimalVal"]);
        Assert.Equal(new DateTime(2023, 12, 25, 10, 30, 0), result["DateTimeVal"]);
    }
    
    public void Dispose()
    {
        _database?.Dispose();
        if (File.Exists(_testDbPath))
        {
            File.Delete(_testDbPath);
        }
    }
}
