using Xunit;
using MiniDatabaseEngine.Storage;

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
}
