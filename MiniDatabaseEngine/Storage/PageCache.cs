using System.Collections.Concurrent;

namespace MiniDatabaseEngine.Storage;

/// <summary>
/// LRU cache for database pages
/// </summary>
public class PageCache
{
    private readonly int _capacity;
    private readonly ConcurrentDictionary<int, CacheNode> _cache;
    private readonly Lock _lockObject = new();
    
    private CacheNode? _head;
    private CacheNode? _tail;
    
    public PageCache(int capacity)
    {
        _capacity = capacity;
        _cache = new ConcurrentDictionary<int, CacheNode>();
    }
    
    public Page? Get(int pageId)
    {
        if (_cache.TryGetValue(pageId, out var node))
        {
            lock (_lockObject)
            {
                MoveToHead(node);
            }
            return node.Page;
        }
        return null;
    }
    
    public void Put(int pageId, Page page)
    {
        lock (_lockObject)
        {
            if (_cache.TryGetValue(pageId, out var node))
            {
                node.Page = page;
                MoveToHead(node);
                return;
            }
            
            var newNode = new CacheNode(page);
            _cache[pageId] = newNode;
            AddToHead(newNode);
            
            if (_cache.Count > _capacity)
            {
                var removed = RemoveTail();
                if (removed != null)
                {
                    _cache.TryRemove(removed.Page.PageId, out _);
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
    
    public IEnumerable<Page> GetDirtyPages()
    {
        return _cache.Values.Where(n => n.Page.IsDirty).Select(n => n.Page);
    }
    
    private void AddToHead(CacheNode node)
    {
        node.Next = _head;
        node.Previous = null;
        
        if (_head != null)
            _head.Previous = node;
            
        _head = node;
        
        if (_tail == null)
            _tail = node;
    }
    
    private void MoveToHead(CacheNode node)
    {
        RemoveNode(node);
        AddToHead(node);
    }
    
    private void RemoveNode(CacheNode node)
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
    
    private CacheNode? RemoveTail()
    {
        if (_tail == null)
            return null;
            
        var node = _tail;
        RemoveNode(node);
        return node;
    }
    
    private class CacheNode
    {
        public Page Page { get; set; }
        public CacheNode? Next { get; set; }
        public CacheNode? Previous { get; set; }
        
        public CacheNode(Page page)
        {
            Page = page;
        }
    }
}
