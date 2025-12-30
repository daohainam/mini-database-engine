using System.IO.MemoryMappedFiles;
using System.Text;

namespace MiniDatabaseEngine.Storage;

/// <summary>
/// Manages database file storage with optional memory-mapped file support
/// </summary>
public class StorageEngine : IDisposable
{
    private readonly string _filePath;
    private readonly FileStream _fileStream;
    private readonly PageCache _cache;
    private readonly bool _useMemoryMappedFile;
    private MemoryMappedFile? _memoryMappedFile;
    private readonly ReaderWriterLockSlim _lock;
    
    public const int HeaderPageId = 0;
    private const int MagicNumber = 0x4D4445; // "MDE" in hex
    
    public StorageEngine(string filePath, int cacheSize = 100, bool useMemoryMappedFile = false)
    {
        _filePath = filePath;
        _cache = new PageCache(cacheSize);
        _useMemoryMappedFile = useMemoryMappedFile;
        _lock = new ReaderWriterLockSlim();
        
        bool isNewFile = !File.Exists(filePath);
        
        _fileStream = new FileStream(
            filePath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.Read);
        
        if (isNewFile)
        {
            InitializeNewDatabase();
        }
        
        if (_useMemoryMappedFile && _fileStream.Length > 0)
        {
            _memoryMappedFile = MemoryMappedFile.CreateFromFile(
                _fileStream,
                null,
                0,
                MemoryMappedFileAccess.ReadWrite,
                HandleInheritability.None,
                false);
        }
    }
    
    private void InitializeNewDatabase()
    {
        // Create header page
        var header = new Page(HeaderPageId);
        using var ms = new MemoryStream(header.Data);
        using var writer = new BinaryWriter(ms);
        
        writer.Write(MagicNumber); // Magic number
        writer.Write(1); // Version
        writer.Write(1); // Next available page ID
        writer.Write(0); // Number of tables
        
        header.IsDirty = true;
        WritePage(header);
        Flush();
    }
    
    public Page ReadPage(int pageId)
    {
        _lock.EnterReadLock();
        try
        {
            // Check cache first
            var cachedPage = _cache.Get(pageId);
            if (cachedPage != null)
                return cachedPage;
            
            var page = new Page(pageId);
            
            if (_useMemoryMappedFile && _memoryMappedFile != null)
            {
                using var accessor = _memoryMappedFile.CreateViewAccessor(
                    pageId * Page.PageSize,
                    Page.PageSize,
                    MemoryMappedFileAccess.Read);
                    
                accessor.ReadArray(0, page.Data, 0, Page.PageSize);
            }
            else
            {
                _fileStream.Seek(pageId * Page.PageSize, SeekOrigin.Begin);
                int totalRead = 0;
                while (totalRead < Page.PageSize)
                {
                    int read = _fileStream.Read(page.Data, totalRead, Page.PageSize - totalRead);
                    if (read == 0)
                        break;
                    totalRead += read;
                }
            }
            
            _cache.Put(pageId, page);
            return page;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }
    
    public void WritePage(Page page)
    {
        _lock.EnterWriteLock();
        try
        {
            page.IsDirty = true;
            _cache.Put(page.PageId, page);
            
            if (_useMemoryMappedFile && _memoryMappedFile != null)
            {
                using var accessor = _memoryMappedFile.CreateViewAccessor(
                    page.PageId * Page.PageSize,
                    Page.PageSize,
                    MemoryMappedFileAccess.Write);
                    
                accessor.WriteArray(0, page.Data, 0, Page.PageSize);
            }
            else
            {
                _fileStream.Seek(page.PageId * Page.PageSize, SeekOrigin.Begin);
                _fileStream.Write(page.Data, 0, Page.PageSize);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
    
    public int AllocatePage()
    {
        _lock.EnterWriteLock();
        try
        {
            var header = ReadPage(HeaderPageId);
            using var ms = new MemoryStream(header.Data);
            using var reader = new BinaryReader(ms);
            
            reader.ReadInt32(); // Magic number
            reader.ReadInt32(); // Version
            int nextPageId = reader.ReadInt32();
            
            // Update next page ID in header
            ms.Seek(8, SeekOrigin.Begin);
            using var writer = new BinaryWriter(ms);
            writer.Write(nextPageId + 1);
            
            header.IsDirty = true;
            WritePage(header);
            
            // Ensure file is large enough
            long requiredSize = (nextPageId + 1) * Page.PageSize;
            if (_fileStream.Length < requiredSize)
            {
                _fileStream.SetLength(requiredSize);
            }
            
            return nextPageId;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
    
    public void Flush()
    {
        _lock.EnterWriteLock();
        try
        {
            var dirtyPages = _cache.GetDirtyPages().ToList();
            foreach (var page in dirtyPages)
            {
                if (_useMemoryMappedFile && _memoryMappedFile != null)
                {
                    using var accessor = _memoryMappedFile.CreateViewAccessor(
                        page.PageId * Page.PageSize,
                        Page.PageSize,
                        MemoryMappedFileAccess.Write);
                        
                    accessor.WriteArray(0, page.Data, 0, Page.PageSize);
                }
                else
                {
                    _fileStream.Seek(page.PageId * Page.PageSize, SeekOrigin.Begin);
                    _fileStream.Write(page.Data, 0, Page.PageSize);
                }
                
                page.IsDirty = false;
            }
            
            _fileStream.Flush(true);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
    
    public void Dispose()
    {
        Flush();
        _memoryMappedFile?.Dispose();
        _fileStream?.Dispose();
        _lock?.Dispose();
    }
}
