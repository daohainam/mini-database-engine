namespace MiniDatabaseEngine.Storage;

/// <summary>
/// Represents a page in the database file
/// </summary>
public class Page
{
    public const int PageSize = 4096; // 4KB pages
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
        PageId = pageId;
        Data = data;
        IsDirty = false;
    }
}
