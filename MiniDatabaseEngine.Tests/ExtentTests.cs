using Xunit;
using MiniDatabaseEngine.Storage;

namespace MiniDatabaseEngine.Tests;

public class ExtentTests
{
    [Fact]
    public void Extent_Creates_With_8_Pages()
    {
        var extent = new Extent(0);
        
        Assert.Equal(0, extent.ExtentId);
        Assert.Equal(8, extent.Pages.Length);
        Assert.Equal(0, extent.StartPageId);
        Assert.Equal(7, extent.EndPageId);
    }
    
    [Fact]
    public void Extent_GetPage_Returns_Correct_Page()
    {
        var extent = new Extent(1);
        
        // Extent 1 should have pages 8-15
        var page = extent.GetPage(10);
        
        Assert.Equal(10, page.PageId);
    }
    
    [Fact]
    public void Extent_ContainsPage_Returns_True_For_Pages_In_Extent()
    {
        var extent = new Extent(0);
        
        Assert.True(extent.ContainsPage(0));
        Assert.True(extent.ContainsPage(7));
        Assert.False(extent.ContainsPage(8));
        Assert.False(extent.ContainsPage(-1));
    }
    
    [Fact]
    public void Extent_GetExtentId_Calculates_Correctly()
    {
        Assert.Equal(0, Extent.GetExtentId(0));
        Assert.Equal(0, Extent.GetExtentId(7));
        Assert.Equal(1, Extent.GetExtentId(8));
        Assert.Equal(1, Extent.GetExtentId(15));
        Assert.Equal(2, Extent.GetExtentId(16));
    }
    
    [Fact]
    public void Extent_IsDirty_Returns_True_When_Any_Page_Dirty()
    {
        var extent = new Extent(0);
        
        Assert.False(extent.IsDirty);
        
        extent.Pages[3].IsDirty = true;
        
        Assert.True(extent.IsDirty);
    }
}

public class ExtentCacheTests
{
    [Fact]
    public void ExtentCache_Put_And_Get_Extent()
    {
        var cache = new ExtentCache(10);
        var extent = new Extent(5);
        
        cache.PutExtent(5, extent);
        var retrieved = cache.GetExtent(5);
        
        Assert.NotNull(retrieved);
        Assert.Equal(5, retrieved.ExtentId);
    }
    
    [Fact]
    public void ExtentCache_GetPage_Returns_Page_From_Cached_Extent()
    {
        var cache = new ExtentCache(10);
        var extent = new Extent(1);
        extent.Pages[2].Data[0] = 42;
        
        cache.PutExtent(1, extent);
        
        var page = cache.GetPage(10); // Page 10 is at index 2 in extent 1
        
        Assert.NotNull(page);
        Assert.Equal(10, page.PageId);
        Assert.Equal(42, page.Data[0]);
    }
    
    [Fact]
    public void ExtentCache_PutPage_Creates_Or_Updates_Extent()
    {
        var cache = new ExtentCache(10);
        var page = new Page(10);
        page.Data[0] = 99;
        
        cache.PutPage(10, page);
        
        var retrieved = cache.GetPage(10);
        Assert.NotNull(retrieved);
        Assert.Equal(99, retrieved.Data[0]);
    }
    
    [Fact]
    public void ExtentCache_Returns_Null_For_Non_Existent_Extent()
    {
        var cache = new ExtentCache(10);
        
        var extent = cache.GetExtent(999);
        
        Assert.Null(extent);
    }
    
    [Fact]
    public void ExtentCache_Evicts_LRU_Extent()
    {
        var cache = new ExtentCache(2);
        
        cache.PutExtent(0, new Extent(0));
        cache.PutExtent(1, new Extent(1));
        cache.PutExtent(2, new Extent(2)); // Should evict extent 0
        
        Assert.Null(cache.GetExtent(0));
        Assert.NotNull(cache.GetExtent(1));
        Assert.NotNull(cache.GetExtent(2));
    }
    
    [Fact]
    public void ExtentCache_Clear_Removes_All_Extents()
    {
        var cache = new ExtentCache(10);
        cache.PutExtent(0, new Extent(0));
        cache.PutExtent(1, new Extent(1));
        
        cache.Clear();
        
        Assert.Null(cache.GetExtent(0));
        Assert.Null(cache.GetExtent(1));
    }
    
    [Fact]
    public void ExtentCache_GetDirtyPages_Returns_All_Dirty_Pages()
    {
        var cache = new ExtentCache(10);
        var extent1 = new Extent(0);
        extent1.Pages[0].IsDirty = true;
        extent1.Pages[3].IsDirty = true;
        
        var extent2 = new Extent(1);
        extent2.Pages[5].IsDirty = true;
        
        cache.PutExtent(0, extent1);
        cache.PutExtent(1, extent2);
        
        var dirtyPages = cache.GetDirtyPages().ToList();
        
        Assert.Equal(3, dirtyPages.Count);
    }
}

public class StorageEngineExtentTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly StorageEngine _storage;
    
    public StorageEngineExtentTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"test_extent_{Guid.NewGuid()}.mde");
        _storage = new StorageEngine(_testDbPath, cacheSize: 100, useMemoryMappedFile: false, useExtentCache: true);
    }
    
    [Fact]
    public void StorageEngine_ReadPage_Uses_Extent_Cache()
    {
        // Write some data to a page
        var page = new Page(10);
        page.Data[0] = 123;
        _storage.WritePage(page);
        _storage.Flush();
        
        // Read it back - should use extent cache
        var readPage = _storage.ReadPage(10);
        
        Assert.Equal(123, readPage.Data[0]);
    }
    
    [Fact]
    public void StorageEngine_ReadExtent_Reads_8_Pages()
    {
        // Write data to multiple pages in an extent
        for (int i = 0; i < 8; i++)
        {
            var page = new Page(8 + i); // Extent 1
            page.Data[0] = (byte)(100 + i);
            _storage.WritePage(page);
        }
        _storage.Flush();
        
        // Read the entire extent
        var extent = _storage.ReadExtent(1);
        
        Assert.Equal(1, extent.ExtentId);
        Assert.Equal(8, extent.Pages.Length);
        
        for (int i = 0; i < 8; i++)
        {
            Assert.Equal(100 + i, extent.Pages[i].Data[0]);
        }
    }
    
    [Fact]
    public void StorageEngine_WriteExtent_Writes_All_Dirty_Pages()
    {
        var extent = new Extent(2);
        extent.Pages[0].IsDirty = true;
        extent.Pages[0].Data[0] = 50;
        extent.Pages[7].IsDirty = true;
        extent.Pages[7].Data[0] = 57;
        
        _storage.WriteExtent(extent);
        _storage.Flush();
        
        // Read back individual pages
        var page0 = _storage.ReadPage(16);
        var page7 = _storage.ReadPage(23);
        
        Assert.Equal(50, page0.Data[0]);
        Assert.Equal(57, page7.Data[0]);
    }
    
    [Fact]
    public void StorageEngine_With_Extent_Cache_Disabled_Still_Works()
    {
        using var storage = new StorageEngine(
            Path.Combine(Path.GetTempPath(), $"test_no_extent_{Guid.NewGuid()}.mde"),
            cacheSize: 100,
            useMemoryMappedFile: false,
            useExtentCache: false);
        
        var page = new Page(5);
        page.Data[0] = 77;
        storage.WritePage(page);
        storage.Flush();
        
        var readPage = storage.ReadPage(5);
        Assert.Equal(77, readPage.Data[0]);
    }
    
    public void Dispose()
    {
        _storage?.Dispose();
        if (File.Exists(_testDbPath))
        {
            File.Delete(_testDbPath);
        }
    }
}
