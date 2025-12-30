using Xunit;

namespace MiniDatabaseEngine.Tests;

public class RecoveryTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly List<string> _filesToCleanup;

    public RecoveryTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"test_recovery_{Guid.NewGuid()}.mde");
        _filesToCleanup = new List<string>();
    }

    [Fact]
    public void Recovery_Replays_Committed_Transactions()
    {
        // Phase 1: Create database and perform transaction
        using (var db = new Database(_testDbPath))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String),
                new("Balance", DataType.Double)
            };
            
            var table = db.CreateTable("Accounts", columns, "Id");
            
            // Insert with committed transaction
            using (var txn = db.BeginTransaction())
            {
                var row = new DataRow(table.Schema);
                row["Id"] = 1;
                row["Name"] = "Alice";
                row["Balance"] = 1000.0;
                
                db.Insert("Accounts", row, txn);
                txn.Commit();
            }
            
            // Flush to persist data
            db.Flush();
        }
        
        // Phase 2: Reopen database and recreate table (simulates recovery)
        using (var db = new Database(_testDbPath))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String),
                new("Balance", DataType.Double)
            };
            
            var table = db.CreateTable("Accounts", columns, "Id");
            
            // Data should be recovered from WAL
            var result = table.SelectByKey(1);
            Assert.NotNull(result);
            Assert.Equal("Alice", result["Name"]);
            Assert.Equal(1000.0, result["Balance"]);
        }
    }

    [Fact]
    public void Recovery_Ignores_Uncommitted_Transactions()
    {
        // Phase 1: Create database with uncommitted transaction
        using (var db = new Database(_testDbPath))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String)
            };
            
            var table = db.CreateTable("Users", columns, "Id");
            
            // Insert with committed transaction
            using (var txn1 = db.BeginTransaction())
            {
                var row1 = new DataRow(table.Schema);
                row1["Id"] = 1;
                row1["Name"] = "Alice";
                db.Insert("Users", row1, txn1);
                txn1.Commit();
            }
            
            // Insert with uncommitted transaction
            using (var txn2 = db.BeginTransaction())
            {
                var row2 = new DataRow(table.Schema);
                row2["Id"] = 2;
                row2["Name"] = "Bob";
                db.Insert("Users", row2, txn2);
                // Don't commit - simulate crash
            }
            
            db.Flush();
        }
        
        // Phase 2: Reopen database
        using (var db = new Database(_testDbPath))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String)
            };
            
            var table = db.CreateTable("Users", columns, "Id");
            
            // Committed data should exist
            var alice = table.SelectByKey(1);
            Assert.NotNull(alice);
            Assert.Equal("Alice", alice["Name"]);
            
            // Uncommitted data should not exist
            var bob = table.SelectByKey(2);
            Assert.Null(bob);
        }
    }

    [Fact]
    public void Recovery_Handles_Multiple_Committed_Transactions()
    {
        // Phase 1: Multiple transactions
        using (var db = new Database(_testDbPath))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String),
                new("Age", DataType.Int)
            };
            
            var table = db.CreateTable("Users", columns, "Id");
            
            // Transaction 1
            using (var txn1 = db.BeginTransaction())
            {
                var row1 = new DataRow(table.Schema);
                row1["Id"] = 1;
                row1["Name"] = "Alice";
                row1["Age"] = 30;
                db.Insert("Users", row1, txn1);
                txn1.Commit();
            }
            
            // Transaction 2
            using (var txn2 = db.BeginTransaction())
            {
                var row2 = new DataRow(table.Schema);
                row2["Id"] = 2;
                row2["Name"] = "Bob";
                row2["Age"] = 25;
                db.Insert("Users", row2, txn2);
                txn2.Commit();
            }
            
            // Transaction 3
            using (var txn3 = db.BeginTransaction())
            {
                var row3 = new DataRow(table.Schema);
                row3["Id"] = 3;
                row3["Name"] = "Charlie";
                row3["Age"] = 35;
                db.Insert("Users", row3, txn3);
                txn3.Commit();
            }
            
            db.Flush();
        }
        
        // Phase 2: Reopen and verify all data
        using (var db = new Database(_testDbPath))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String),
                new("Age", DataType.Int)
            };
            
            var table = db.CreateTable("Users", columns, "Id");
            
            var alice = table.SelectByKey(1);
            Assert.NotNull(alice);
            Assert.Equal("Alice", alice["Name"]);
            
            var bob = table.SelectByKey(2);
            Assert.NotNull(bob);
            Assert.Equal("Bob", bob["Name"]);
            
            var charlie = table.SelectByKey(3);
            Assert.NotNull(charlie);
            Assert.Equal("Charlie", charlie["Name"]);
        }
    }

    [Fact]
    public void Recovery_Handles_Updates_And_Deletes()
    {
        // Phase 1: Insert, update, and delete operations
        using (var db = new Database(_testDbPath))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String),
                new("Status", DataType.String)
            };
            
            var table = db.CreateTable("Records", columns, "Id");
            
            // Initial insert
            using (var txn1 = db.BeginTransaction())
            {
                for (int i = 1; i <= 3; i++)
                {
                    var row = new DataRow(table.Schema);
                    row["Id"] = i;
                    row["Name"] = $"Record{i}";
                    row["Status"] = "Active";
                    db.Insert("Records", row, txn1);
                }
                txn1.Commit();
            }
            
            // Update one record
            using (var txn2 = db.BeginTransaction())
            {
                var updated = new DataRow(table.Schema);
                updated["Id"] = 2;
                updated["Name"] = "Record2 Updated";
                updated["Status"] = "Modified";
                db.Update("Records", 2, updated, txn2);
                txn2.Commit();
            }
            
            // Delete one record
            using (var txn3 = db.BeginTransaction())
            {
                db.Delete("Records", 3, txn3);
                txn3.Commit();
            }
            
            db.Flush();
        }
        
        // Phase 2: Verify recovery
        using (var db = new Database(_testDbPath))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String),
                new("Status", DataType.String)
            };
            
            var table = db.CreateTable("Records", columns, "Id");
            
            // First record unchanged
            var record1 = table.SelectByKey(1);
            Assert.NotNull(record1);
            Assert.Equal("Record1", record1["Name"]);
            Assert.Equal("Active", record1["Status"]);
            
            // Second record updated
            var record2 = table.SelectByKey(2);
            Assert.NotNull(record2);
            Assert.Equal("Record2 Updated", record2["Name"]);
            Assert.Equal("Modified", record2["Status"]);
            
            // Third record deleted
            var record3 = table.SelectByKey(3);
            Assert.Null(record3);
        }
    }

    [Fact]
    public void Recovery_After_Checkpoint_Works()
    {
        // Phase 1: Insert data and checkpoint
        using (var db = new Database(_testDbPath))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String)
            };
            
            var table = db.CreateTable("Users", columns, "Id");
            
            // Insert with transaction
            using (var txn = db.BeginTransaction())
            {
                var row = new DataRow(table.Schema);
                row["Id"] = 1;
                row["Name"] = "Alice";
                db.Insert("Users", row, txn);
                txn.Commit();
            }
            
            // Checkpoint
            db.Checkpoint();
            
            // Insert more data after checkpoint
            using (var txn = db.BeginTransaction())
            {
                var row = new DataRow(table.Schema);
                row["Id"] = 2;
                row["Name"] = "Bob";
                db.Insert("Users", row, txn);
                txn.Commit();
            }
            
            db.Flush();
        }
        
        // Phase 2: Verify recovery
        using (var db = new Database(_testDbPath))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String)
            };
            
            var table = db.CreateTable("Users", columns, "Id");
            
            // Both records should exist
            var alice = table.SelectByKey(1);
            Assert.NotNull(alice);
            Assert.Equal("Alice", alice["Name"]);
            
            var bob = table.SelectByKey(2);
            Assert.NotNull(bob);
            Assert.Equal("Bob", bob["Name"]);
        }
    }

    [Fact]
    public void Recovery_With_Rolled_Back_Transaction()
    {
        // Phase 1: Committed and rolled back transactions
        using (var db = new Database(_testDbPath))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String)
            };
            
            var table = db.CreateTable("Users", columns, "Id");
            
            // Committed transaction
            using (var txn1 = db.BeginTransaction())
            {
                var row1 = new DataRow(table.Schema);
                row1["Id"] = 1;
                row1["Name"] = "Alice";
                db.Insert("Users", row1, txn1);
                txn1.Commit();
            }
            
            // Rolled back transaction
            using (var txn2 = db.BeginTransaction())
            {
                var row2 = new DataRow(table.Schema);
                row2["Id"] = 2;
                row2["Name"] = "Bob";
                db.Insert("Users", row2, txn2);
                txn2.Rollback();
            }
            
            // Another committed transaction
            using (var txn3 = db.BeginTransaction())
            {
                var row3 = new DataRow(table.Schema);
                row3["Id"] = 3;
                row3["Name"] = "Charlie";
                db.Insert("Users", row3, txn3);
                txn3.Commit();
            }
            
            db.Flush();
        }
        
        // Phase 2: Verify recovery
        using (var db = new Database(_testDbPath))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String)
            };
            
            var table = db.CreateTable("Users", columns, "Id");
            
            // First committed transaction
            var alice = table.SelectByKey(1);
            Assert.NotNull(alice);
            
            // Rolled back transaction
            var bob = table.SelectByKey(2);
            Assert.Null(bob);
            
            // Second committed transaction
            var charlie = table.SelectByKey(3);
            Assert.NotNull(charlie);
        }
    }

    public void Dispose()
    {
        // Clean up test files
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
        
        var walPath = Path.ChangeExtension(_testDbPath, ".wal");
        if (File.Exists(walPath))
            File.Delete(walPath);
        
        foreach (var file in _filesToCleanup)
        {
            if (File.Exists(file))
                File.Delete(file);
        }
    }
}
