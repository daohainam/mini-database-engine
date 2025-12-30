using Xunit;

namespace MiniDatabaseEngine.Tests;

public class ConcurrencyTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly Database _database;
    
    public ConcurrencyTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.mde");
        _database = new Database(_testDbPath);
    }
    
    [Fact]
    public void Concurrent_Inserts_Are_Thread_Safe()
    {
        var columns = new List<ColumnDefinition>
        {
            new ColumnDefinition("Id", DataType.Int, false),
            new ColumnDefinition("Value", DataType.String)
        };
        
        var table = _database.CreateTable("ConcurrentTest", columns, "Id");
        
        var tasks = new List<Task>();
        for (int i = 0; i < 10; i++)
        {
            int id = i;
            tasks.Add(Task.Run(() =>
            {
                var row = new DataRow(table.Schema);
                row["Id"] = id;
                row["Value"] = $"Value{id}";
                _database.Insert("ConcurrentTest", row);
            }));
        }
        
        Task.WaitAll(tasks.ToArray());
        
        var results = table.SelectAll().ToList();
        Assert.Equal(10, results.Count);
    }
    
    [Fact]
    public void Concurrent_Reads_And_Writes_Are_Thread_Safe()
    {
        var columns = new List<ColumnDefinition>
        {
            new ColumnDefinition("Id", DataType.Int, false),
            new ColumnDefinition("Counter", DataType.Int)
        };
        
        var table = _database.CreateTable("ReadWriteTest", columns, "Id");
        
        // Insert initial data
        var row = new DataRow(table.Schema);
        row["Id"] = 1;
        row["Counter"] = 0;
        _database.Insert("ReadWriteTest", row);
        
        var tasks = new List<Task>();
        
        // Writers
        for (int i = 0; i < 5; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 10; j++)
                {
                    var current = table.SelectByKey(1);
                    if (current != null)
                    {
                        var counter = (int)(current["Counter"] ?? 0);
                        var updated = new DataRow(table.Schema);
                        updated["Id"] = 1;
                        updated["Counter"] = counter + 1;
                        _database.Update("ReadWriteTest", 1, updated);
                    }
                }
            }));
        }
        
        // Readers
        for (int i = 0; i < 5; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 20; j++)
                {
                    var current = table.SelectByKey(1);
                    Assert.NotNull(current);
                }
            }));
        }
        
        Task.WaitAll(tasks.ToArray());
        
        var finalRow = table.SelectByKey(1);
        Assert.NotNull(finalRow);
        // Counter should be positive (actual value may vary due to race conditions, but data should be consistent)
        Assert.True((int)(finalRow["Counter"] ?? 0) > 0);
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
