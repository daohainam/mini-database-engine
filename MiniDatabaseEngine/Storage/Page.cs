namespace MiniDatabaseEngine.Storage;

/// <summary>
/// Represents a page in the database file
/// </summary>
public class Page
{
#if DATA_PAGE_SIZE
    public const int PageSize = #DATA_PAGE_SIZE#; 
#else
    public const int PageSize = 4096; // 4KB pages
#endif
    public int PageId { get; set; }
    public byte[] Data { get; set; }
    public bool IsDirty { get; set; }
    
    public Page(int pageId)
    {
        PageId = pageId;
        Data = new byte[PageSize];
        IsDirty = false;
    }
    
    public Page(int pageId, byte[] data)
    {
        if (data == null)
            throw new ArgumentNullException(nameof(data));
        if (data.Length != PageSize)
            throw new ArgumentException($"Data must be exactly {PageSize} bytes, got {data.Length}", nameof(data));
        
        PageId = pageId;
        Data = data;
        IsDirty = false;
    }
}
