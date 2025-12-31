namespace MiniDatabaseEngine.Storage;

/// <summary>
/// Represents an extent in the database file - a group of 8 consecutive pages
/// </summary>
public class Extent
{
    public const int PagesPerExtent = 8;
    
    /// <summary>
    /// The ID of this extent (extentId * PagesPerExtent = first page ID)
    /// </summary>
    public int ExtentId { get; set; }
    
    /// <summary>
    /// The 8 pages contained in this extent
    /// </summary>
    public Page[] Pages { get; set; }
    
    /// <summary>
    /// Indicates whether any page in this extent has been modified
    /// </summary>
    public bool IsDirty => Pages.Any(p => p.IsDirty);
    
    public Extent(int extentId)
    {
        ExtentId = extentId;
        Pages = new Page[PagesPerExtent];
        
        // Initialize pages for this extent
        for (int i = 0; i < PagesPerExtent; i++)
        {
            int pageId = extentId * PagesPerExtent + i;
            Pages[i] = new Page(pageId);
        }
    }
    
    /// <summary>
    /// Gets the first page ID in this extent
    /// </summary>
    public int StartPageId => ExtentId * PagesPerExtent;
    
    /// <summary>
    /// Gets the last page ID in this extent
    /// </summary>
    public int EndPageId => ExtentId * PagesPerExtent + PagesPerExtent - 1;
    
    /// <summary>
    /// Gets a specific page from this extent by page ID
    /// </summary>
    public Page GetPage(int pageId)
    {
        int index = pageId - StartPageId;
        if (index < 0 || index >= PagesPerExtent)
            throw new ArgumentOutOfRangeException(nameof(pageId), $"Page {pageId} is not in extent {ExtentId}");
        
        return Pages[index];
    }
    
    /// <summary>
    /// Checks if a page ID belongs to this extent
    /// </summary>
    public bool ContainsPage(int pageId)
    {
        return pageId >= StartPageId && pageId <= EndPageId;
    }
    
    /// <summary>
    /// Gets the extent ID for a given page ID
    /// </summary>
    public static int GetExtentId(int pageId)
    {
        return pageId / PagesPerExtent;
    }
}
