using System.Collections.Concurrent;

namespace MiniDatabaseEngine.Storage;

/// <summary>
/// LRU cache for database extents (groups of 8 pages)
/// Provides better caching by loading and managing pages in extent units
/// </summary>
public class ExtentCache
{
    private readonly int _capacity;
    private readonly ConcurrentDictionary<int, ExtentCacheNode> _cache;
    private readonly Lock _lockObject = new();
    
    private ExtentCacheNode? _head;
    private ExtentCacheNode? _tail;
    
    public ExtentCache(int capacity)
    {
        _capacity = capacity;
        _cache = new ConcurrentDictionary<int, ExtentCacheNode>();
    }
    
    /// <summary>
    /// Gets an extent by extent ID
    /// </summary>
    public Extent? GetExtent(int extentId)
    {
        if (_cache.TryGetValue(extentId, out var node))
        {
            lock (_lockObject)
            {
                MoveToHead(node);
            }
            return node.Extent;
        }
        return null;
    }
    
    /// <summary>
    /// Gets a specific page by page ID, loading its extent if necessary
    /// </summary>
    public Page? GetPage(int pageId)
    {
        int extentId = Extent.GetExtentId(pageId);
        var extent = GetExtent(extentId);
        
        if (extent != null)
        {
            return extent.GetPage(pageId);
        }
        
        return null;
    }
    
    /// <summary>
    /// Puts an extent into the cache
    /// </summary>
    public void PutExtent(int extentId, Extent extent)
    {
        lock (_lockObject)
        {
            if (_cache.TryGetValue(extentId, out var node))
            {
                node.Extent = extent;
                MoveToHead(node);
                return;
            }
            
            var newNode = new ExtentCacheNode(extent);
            _cache[extentId] = newNode;
            AddToHead(newNode);
            
            if (_cache.Count > _capacity)
            {
                var removed = RemoveTail();
                if (removed != null)
                {
                    _cache.TryRemove(removed.Extent.ExtentId, out _);
                }
            }
        }
    }
    
    /// <summary>
    /// Puts a page into the cache, loading or creating its extent
    /// </summary>
    public void PutPage(int pageId, Page page)
    {
        int extentId = Extent.GetExtentId(pageId);
        
        lock (_lockObject)
        {
            if (_cache.TryGetValue(extentId, out var node))
            {
                // Update the page in the existing extent
                int index = pageId - node.Extent.StartPageId;
                node.Extent.Pages[index] = page;
                MoveToHead(node);
            }
            else
            {
                // Create a new extent with this page
                var extent = new Extent(extentId);
                int index = pageId - extent.StartPageId;
                extent.Pages[index] = page;
                
                var newNode = new ExtentCacheNode(extent);
                _cache[extentId] = newNode;
                AddToHead(newNode);
                
                if (_cache.Count > _capacity)
                {
                    var removed = RemoveTail();
                    if (removed != null)
                    {
                        _cache.TryRemove(removed.Extent.ExtentId, out _);
                    }
                }
            }
        }
    }
    
    public void Clear()
    {
        lock (_lockObject)
        {
            _cache.Clear();
            _head = null;
            _tail = null;
        }
    }
    
    /// <summary>
    /// Gets all dirty pages across all cached extents
    /// </summary>
    public IEnumerable<Page> GetDirtyPages()
    {
        return _cache.Values
            .SelectMany(n => n.Extent.Pages)
            .Where(p => p.IsDirty);
    }
    
    /// <summary>
    /// Gets all dirty extents
    /// </summary>
    public IEnumerable<Extent> GetDirtyExtents()
    {
        return _cache.Values
            .Where(n => n.Extent.IsDirty)
            .Select(n => n.Extent);
    }
    
    private void AddToHead(ExtentCacheNode node)
    {
        node.Next = _head;
        node.Previous = null;
        
        if (_head != null)
            _head.Previous = node;
            
        _head = node;
        
        if (_tail == null)
            _tail = node;
    }
    
    private void MoveToHead(ExtentCacheNode node)
    {
        RemoveNode(node);
        AddToHead(node);
    }
    
    private void RemoveNode(ExtentCacheNode node)
    {
        if (node.Previous != null)
            node.Previous.Next = node.Next;
        else
            _head = node.Next;
            
        if (node.Next != null)
            node.Next.Previous = node.Previous;
        else
            _tail = node.Previous;
    }
    
    private ExtentCacheNode? RemoveTail()
    {
        if (_tail == null)
            return null;
            
        var node = _tail;
        RemoveNode(node);
        return node;
    }
    
    private class ExtentCacheNode
    {
        public Extent Extent { get; set; }
        public ExtentCacheNode? Next { get; set; }
        public ExtentCacheNode? Previous { get; set; }
        
        public ExtentCacheNode(Extent extent)
        {
            Extent = extent;
        }
    }
}
