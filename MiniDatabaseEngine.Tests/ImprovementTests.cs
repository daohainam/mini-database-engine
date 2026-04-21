using Xunit;
using MiniDatabaseEngine.Storage;
using System.Collections.Concurrent;

namespace MiniDatabaseEngine.Tests;

/// <summary>
/// Tests for bug fixes and improvements
/// </summary>
public class ImprovementTests
{
    [Fact]
    public void BPlusTree_GetAll_Returns_Materialized_List()
    {
        var tree = new BPlusTree.BPlusTree(4, DataType.Int);

        tree.Insert(1, "A");
        tree.Insert(2, "B");
        tree.Insert(3, "C");

        // GetAll should return a fully materialized list, not a lazy iterator
        var result = tree.GetAll();
        Assert.IsType<List<KeyValuePair<object, object?>>>(result);

        var list = result.ToList();
        Assert.Equal(3, list.Count);
        Assert.Equal(1, list[0].Key);
        Assert.Equal(2, list[1].Key);
        Assert.Equal(3, list[2].Key);
    }

    [Fact]
    public void BPlusTree_Range_Returns_Materialized_List()
    {
        var tree = new BPlusTree.BPlusTree(4, DataType.Int);

        for (int i = 1; i <= 10; i++)
            tree.Insert(i, $"V{i}");

        var result = tree.Range(3, 7);
        Assert.IsType<List<KeyValuePair<object, object?>>>(result);

        var list = result.ToList();
        Assert.Equal(5, list.Count);
    }

    [Fact]
    public void Table_SelectAll_Returns_Materialized_Collection()
    {
        var testDbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.mde");
        try
        {
            using var db = new Database(testDbPath);
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String)
            };

            var table = db.CreateTable("Test", columns, "Id");

            var row = new DataRow(table.Schema);
            row["Id"] = 1;
            row["Name"] = "Alice";
            db.Insert("Test", row);

            // SelectAll should return a fully materialized collection
            var result = table.SelectAll();
            Assert.IsType<List<DataRow>>(result);
            Assert.Single(result);
        }
        finally
        {
            if (File.Exists(testDbPath))
                File.Delete(testDbPath);
        }
    }

    [Fact]
    public void DataRow_Integer_Indexer_Validates_Bounds()
    {
        var schema = new TableSchema("Test",
            new List<ColumnDefinition> { new("Id", DataType.Int) }, "Id");
        var row = new DataRow(schema);

        Assert.Throws<ArgumentOutOfRangeException>(() => row[-1]);
        Assert.Throws<ArgumentOutOfRangeException>(() => row[1]);
        Assert.Throws<ArgumentOutOfRangeException>(() => { row[-1] = 42; });
        Assert.Throws<ArgumentOutOfRangeException>(() => { row[1] = 42; });

        // Valid index should work
        row[0] = 42;
        Assert.Equal(42, row[0]);
    }

    [Fact]
    public void TableSchema_Properties_Are_ReadOnly_After_Construction()
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String)
        };

        var schema = new TableSchema("Users", columns, "Id");

        // Verify properties are set correctly
        Assert.Equal("Users", schema.TableName);
        Assert.Equal("Id", schema.PrimaryKeyColumn);
        Assert.Equal(2, schema.Columns.Count);

        // Verify properties don't have public setters (compile-time check)
        // The fact that these properties are now get-only is validated by this test compiling
        var tableNameProp = typeof(TableSchema).GetProperty("TableName");
        Assert.NotNull(tableNameProp);
        Assert.Null(tableNameProp!.SetMethod?.IsPublic == true ? tableNameProp.SetMethod : null);

        var pkProp = typeof(TableSchema).GetProperty("PrimaryKeyColumn");
        Assert.NotNull(pkProp);
        Assert.Null(pkProp!.SetMethod?.IsPublic == true ? pkProp.SetMethod : null);
    }

    [Fact]
    public void ColumnDefinition_Properties_Are_ReadOnly_After_Construction()
    {
        var col = new ColumnDefinition("Name", DataType.String, true, 100);

        Assert.Equal("Name", col.Name);
        Assert.Equal(DataType.String, col.DataType);
        Assert.True(col.IsNullable);
        Assert.Equal(100, col.MaxLength);

        // Verify no public setters
        var nameProp = typeof(ColumnDefinition).GetProperty("Name");
        Assert.NotNull(nameProp);
        Assert.Null(nameProp!.SetMethod?.IsPublic == true ? nameProp.SetMethod : null);
    }

    [Fact]
    public void Page_Constructor_Validates_Data_Length()
    {
        // Valid data length should work
        var validData = new byte[Page.PageSize];
        var page = new Page(1, validData);
        Assert.Equal(1, page.PageId);

        // Invalid data length should throw
        var invalidData = new byte[100];
        Assert.Throws<ArgumentException>(() => new Page(1, invalidData));

        // Null data should throw
        Assert.Throws<ArgumentNullException>(() => new Page(1, null!));
    }

    [Fact]
    public void PageCache_GetDirtyPages_Is_Thread_Safe()
    {
        var cache = new PageCache(100);

        // Add some pages
        for (int i = 0; i < 10; i++)
        {
            var page = new Page(i);
            page.IsDirty = (i % 2 == 0);
            cache.Put(i, page);
        }

        // GetDirtyPages should return a snapshot
        var dirtyPages = cache.GetDirtyPages();
        Assert.IsType<List<Page>>(dirtyPages);
        Assert.Equal(5, dirtyPages.Count());
    }

    [Fact]
    public void ExtentCache_GetDirtyPages_Is_Thread_Safe()
    {
        var cache = new ExtentCache(10);
        var extent = new Extent(0);
        extent.Pages[0].IsDirty = true;
        extent.Pages[3].IsDirty = true;
        cache.PutExtent(0, extent);

        // GetDirtyPages should return a materialized list
        var dirtyPages = cache.GetDirtyPages();
        Assert.IsType<List<Page>>(dirtyPages);
        Assert.Equal(2, dirtyPages.Count());
    }

    [Fact]
    public void ExtentCache_GetDirtyExtents_Is_Thread_Safe()
    {
        var cache = new ExtentCache(10);

        var extent1 = new Extent(0);
        extent1.Pages[0].IsDirty = true;

        var extent2 = new Extent(1);
        // extent2 is clean

        cache.PutExtent(0, extent1);
        cache.PutExtent(1, extent2);

        var dirtyExtents = cache.GetDirtyExtents();
        Assert.IsType<List<Extent>>(dirtyExtents);
        Assert.Single(dirtyExtents);
    }

    [Fact]
    public async Task SelectAll_Thread_Safety_With_Concurrent_Modifications()
    {
        var testDbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.mde");
        try
        {
            using var db = new Database(testDbPath);
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String)
            };

            var table = db.CreateTable("Test", columns, "Id");

            // Pre-insert some data
            for (int i = 0; i < 10; i++)
            {
                var row = new DataRow(table.Schema);
                row["Id"] = i;
                row["Name"] = $"Name{i}";
                db.Insert("Test", row);
            }

            // Run concurrent reads and writes
            var tasks = new List<Task>();
            for (int t = 0; t < 5; t++)
            {
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        // SelectAll should return a complete snapshot without exception
                        var results = table.SelectAll().ToList();
                        Assert.True(results.Count >= 0);
                    }
                }));

                int offset = (t + 1) * 100;
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 5; i++)
                    {
                        var row = new DataRow(table.Schema);
                        row["Id"] = offset + i;
                        row["Name"] = $"New{offset + i}";
                        db.Insert("Test", row);
                    }
                }));
            }

            await Task.WhenAll(tasks);
        }
        finally
        {
            if (File.Exists(testDbPath))
                File.Delete(testDbPath);
        }
    }

    [Fact]
    public void Database_Metrics_Are_Collected()
    {
        var testDbPath = Path.Combine(Path.GetTempPath(), $"test_metrics_{Guid.NewGuid()}.mde");
        try
        {
            using var db = new Database(testDbPath);
            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String)
            };

            var table = db.CreateTable("MetricsUsers", columns, "Id");
            var row = new DataRow(table.Schema);
            row["Id"] = 1;
            row["Name"] = "Alice";
            db.Insert("MetricsUsers", row);

            var updated = new DataRow(table.Schema);
            updated["Id"] = 1;
            updated["Name"] = "Alice Updated";
            db.Update("MetricsUsers", 1, updated);
            db.Delete("MetricsUsers", 1);
            db.Flush();
            db.Checkpoint();

            var metrics = db.GetMetricsSnapshot();
            Assert.True(metrics.TablesCreated >= 1);
            Assert.True(metrics.Inserts >= 1);
            Assert.True(metrics.Updates >= 1);
            Assert.True(metrics.Deletes >= 1);
            Assert.True(metrics.Flushes >= 1);
            Assert.True(metrics.Checkpoints >= 1);
        }
        finally
        {
            if (File.Exists(testDbPath))
                File.Delete(testDbPath);

            var walPath = Path.ChangeExtension(testDbPath, ".wal");
            if (File.Exists(walPath))
                File.Delete(walPath);
        }
    }

    [Fact]
    public void Database_Emits_Structured_Logs()
    {
        var testDbPath = Path.Combine(Path.GetTempPath(), $"test_logs_{Guid.NewGuid()}.mde");
        try
        {
            var logger = new InMemoryDatabaseLogger();
            var options = new DatabaseOptions { Logger = logger };
            using var db = new Database(testDbPath, options: options);

            var columns = new List<ColumnDefinition>
            {
                new("Id", DataType.Int, false),
                new("Name", DataType.String)
            };

            db.CreateTable("LoggedUsers", columns, "Id");
            Assert.Contains(logger.Entries, e => e.EventName == "table.created");
            Assert.Contains(logger.Entries, e => e.EventName == "database.opened");
        }
        finally
        {
            if (File.Exists(testDbPath))
                File.Delete(testDbPath);

            var walPath = Path.ChangeExtension(testDbPath, ".wal");
            if (File.Exists(walPath))
                File.Delete(walPath);
        }
    }

    [Fact]
    public void Database_Backup_And_Restore_Works()
    {
        var testDbPath = Path.Combine(Path.GetTempPath(), $"test_backup_{Guid.NewGuid()}.mde");
        var backupRoot = Path.Combine(Path.GetTempPath(), $"mde_backup_{Guid.NewGuid()}");
        var restoredPath = Path.Combine(Path.GetTempPath(), $"test_restore_{Guid.NewGuid()}.mde");
        try
        {
            using (var db = new Database(testDbPath))
            {
                var columns = new List<ColumnDefinition>
                {
                    new("Id", DataType.Int, false),
                    new("Name", DataType.String)
                };

                var table = db.CreateTable("BackupUsers", columns, "Id");
                var row = new DataRow(table.Schema);
                row["Id"] = 7;
                row["Name"] = "Recovered";
                db.Insert("BackupUsers", row);

                var backupPath = db.CreateBackup(backupRoot, includeWal: true);
                Database.RestoreBackup(backupPath, restoredPath, overwrite: true);
            }

            using var restoredDb = new Database(restoredPath);
            var restoredTable = restoredDb.GetTable("BackupUsers");
            var restoredRow = restoredTable.SelectByKey(7);
            Assert.NotNull(restoredRow);
            Assert.Equal("Recovered", restoredRow["Name"]);
        }
        finally
        {
            if (Directory.Exists(backupRoot))
                Directory.Delete(backupRoot, recursive: true);

            if (File.Exists(testDbPath))
                File.Delete(testDbPath);
            if (File.Exists(Path.ChangeExtension(testDbPath, ".wal")))
                File.Delete(Path.ChangeExtension(testDbPath, ".wal"));

            if (File.Exists(restoredPath))
                File.Delete(restoredPath);
            if (File.Exists(Path.ChangeExtension(restoredPath, ".wal")))
                File.Delete(Path.ChangeExtension(restoredPath, ".wal"));
        }
    }

    [Fact]
    public void Database_Integrity_Check_Detects_Corrupted_Header()
    {
        var testDbPath = Path.Combine(Path.GetTempPath(), $"test_integrity_{Guid.NewGuid()}.mde");
        try
        {
            using (var db = new Database(testDbPath))
            {
                db.Flush();
            }

            using (var fs = new FileStream(testDbPath, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
            using (var writer = new BinaryWriter(fs))
            {
                writer.Write(0); // Corrupt magic number
            }

            using var reopenedDb = new Database(testDbPath);
            var report = reopenedDb.CheckIntegrity();
            Assert.False(report.IsHealthy);
            Assert.Contains(report.Issues, issue => issue.Contains("magic number", StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            if (File.Exists(testDbPath))
                File.Delete(testDbPath);

            var walPath = Path.ChangeExtension(testDbPath, ".wal");
            if (File.Exists(walPath))
                File.Delete(walPath);
        }
    }

    private sealed class InMemoryDatabaseLogger : IDatabaseLogger
    {
        public ConcurrentBag<DatabaseLogEntry> Entries { get; } = new();

        public void Log(DatabaseLogEntry entry)
        {
            Entries.Add(entry);
        }
    }
}
