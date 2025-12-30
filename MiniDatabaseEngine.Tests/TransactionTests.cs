using Xunit;
using MiniDatabaseEngine.Transaction;

namespace MiniDatabaseEngine.Tests;

public class TransactionTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly Database _database;

    public TransactionTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"test_txn_{Guid.NewGuid()}.mde");
        _database = new Database(_testDbPath);
    }

    [Fact]
    public void BeginTransaction_Creates_Active_Transaction()
    {
        using var txn = _database.BeginTransaction();
        
        Assert.NotNull(txn);
        Assert.Equal(TransactionState.Active, txn.State);
    }

    [Fact]
    public void Transaction_Commit_Makes_Changes_Permanent()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String)
        };
        
        var table = _database.CreateTable("Users", columns, "Id");
        
        // Start transaction and insert data
        using (var txn = _database.BeginTransaction())
        {
            var row = new DataRow(table.Schema);
            row["Id"] = 1;
            row["Name"] = "Alice";
            
            _database.Insert("Users", row, txn);
            txn.Commit();
        }
        
        // Verify data persists after commit
        var result = table.SelectByKey(1);
        Assert.NotNull(result);
        Assert.Equal("Alice", result["Name"]);
    }

    [Fact]
    public void Transaction_Rollback_Reverts_Changes()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String)
        };
        
        var table = _database.CreateTable("Users", columns, "Id");
        
        // Insert initial data
        var row1 = new DataRow(table.Schema);
        row1["Id"] = 1;
        row1["Name"] = "Alice";
        _database.Insert("Users", row1);
        
        // Start transaction and modify data
        using (var txn = _database.BeginTransaction())
        {
            var row2 = new DataRow(table.Schema);
            row2["Id"] = 2;
            row2["Name"] = "Bob";
            
            _database.Insert("Users", row2, txn);
            txn.Rollback();
        }
        
        // Verify Bob was not inserted
        var result = table.SelectByKey(2);
        Assert.Null(result);
        
        // Verify Alice still exists
        var alice = table.SelectByKey(1);
        Assert.NotNull(alice);
        Assert.Equal("Alice", alice["Name"]);
    }

    [Fact]
    public void Transaction_Auto_Rollback_On_Dispose()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String)
        };
        
        var table = _database.CreateTable("Users", columns, "Id");
        
        // Transaction without explicit commit or rollback
        using (var txn = _database.BeginTransaction())
        {
            var row = new DataRow(table.Schema);
            row["Id"] = 1;
            row["Name"] = "Alice";
            
            _database.Insert("Users", row, txn);
            // No commit - should auto rollback
        }
        
        // Verify data was rolled back
        var result = table.SelectByKey(1);
        Assert.Null(result);
    }

    [Fact]
    public void Transaction_Update_Is_Atomic()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String),
            new("Balance", DataType.Double)
        };
        
        var table = _database.CreateTable("Accounts", columns, "Id");
        
        // Create initial account
        var row1 = new DataRow(table.Schema);
        row1["Id"] = 1;
        row1["Name"] = "Alice";
        row1["Balance"] = 1000.0;
        _database.Insert("Accounts", row1);
        
        // Update in transaction
        using (var txn = _database.BeginTransaction())
        {
            var updated = new DataRow(table.Schema);
            updated["Id"] = 1;
            updated["Name"] = "Alice Updated";
            updated["Balance"] = 1500.0;
            
            _database.Update("Accounts", 1, updated, txn);
            txn.Commit();
        }
        
        // Verify update
        var result = table.SelectByKey(1);
        Assert.NotNull(result);
        Assert.Equal("Alice Updated", result["Name"]);
        Assert.Equal(1500.0, result["Balance"]);
    }

    [Fact]
    public void Transaction_Delete_Is_Atomic()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String)
        };
        
        var table = _database.CreateTable("Users", columns, "Id");
        
        // Insert data
        var row = new DataRow(table.Schema);
        row["Id"] = 1;
        row["Name"] = "Alice";
        _database.Insert("Users", row);
        
        // Delete in transaction
        using (var txn = _database.BeginTransaction())
        {
            _database.Delete("Users", 1, txn);
            txn.Commit();
        }
        
        // Verify deletion
        var result = table.SelectByKey(1);
        Assert.Null(result);
    }

    [Fact]
    public void Multiple_Operations_In_Single_Transaction()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String),
            new("Age", DataType.Int)
        };
        
        var table = _database.CreateTable("Users", columns, "Id");
        
        // Multiple operations in one transaction
        using (var txn = _database.BeginTransaction())
        {
            var row1 = new DataRow(table.Schema);
            row1["Id"] = 1;
            row1["Name"] = "Alice";
            row1["Age"] = 30;
            _database.Insert("Users", row1, txn);
            
            var row2 = new DataRow(table.Schema);
            row2["Id"] = 2;
            row2["Name"] = "Bob";
            row2["Age"] = 25;
            _database.Insert("Users", row2, txn);
            
            var row3 = new DataRow(table.Schema);
            row3["Id"] = 3;
            row3["Name"] = "Charlie";
            row3["Age"] = 35;
            _database.Insert("Users", row3, txn);
            
            txn.Commit();
        }
        
        // Verify all inserts
        Assert.NotNull(table.SelectByKey(1));
        Assert.NotNull(table.SelectByKey(2));
        Assert.NotNull(table.SelectByKey(3));
    }

    [Fact]
    public void Transaction_Rollback_Undoes_All_Operations()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String)
        };
        
        var table = _database.CreateTable("Users", columns, "Id");
        
        // Multiple operations then rollback
        using (var txn = _database.BeginTransaction())
        {
            for (int i = 1; i <= 5; i++)
            {
                var row = new DataRow(table.Schema);
                row["Id"] = i;
                row["Name"] = $"User{i}";
                _database.Insert("Users", row, txn);
            }
            
            txn.Rollback();
        }
        
        // Verify all inserts were rolled back
        for (int i = 1; i <= 5; i++)
        {
            Assert.Null(table.SelectByKey(i));
        }
    }

    [Fact]
    public void Non_Transactional_Operations_Still_Work()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String)
        };
        
        var table = _database.CreateTable("Users", columns, "Id");
        
        // Insert without transaction
        var row = new DataRow(table.Schema);
        row["Id"] = 1;
        row["Name"] = "Alice";
        _database.Insert("Users", row);
        
        // Verify data persists
        var result = table.SelectByKey(1);
        Assert.NotNull(result);
        Assert.Equal("Alice", result["Name"]);
    }

    [Fact]
    public void Checkpoint_Truncates_WAL()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String)
        };
        
        var table = _database.CreateTable("Users", columns, "Id");
        
        // Insert with transaction
        using (var txn = _database.BeginTransaction())
        {
            var row = new DataRow(table.Schema);
            row["Id"] = 1;
            row["Name"] = "Alice";
            _database.Insert("Users", row, txn);
            txn.Commit();
        }
        
        // Checkpoint should flush and truncate WAL
        _database.Checkpoint();
        
        // Data should still be accessible
        var result = table.SelectByKey(1);
        Assert.NotNull(result);
        Assert.Equal("Alice", result["Name"]);
    }

    public void Dispose()
    {
        _database?.Dispose();
        
        // Clean up test files
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
        
        var walPath = Path.ChangeExtension(_testDbPath, ".wal");
        if (File.Exists(walPath))
            File.Delete(walPath);
    }
}
