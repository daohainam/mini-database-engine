# Module 2: Pages & Extents — The Storage Floor Plan

### Teaching Arc
- **Metaphor:** A **self-storage warehouse**. Every unit is exactly the same size (a *page*: 4096 bytes — 4KB). Units are grouped into blocks of 8 adjacent units (an *extent*) because it's cheaper for the moving truck (the disk) to pick up a whole block than run 8 separate trips. A front-desk clipboard (the page cache) remembers which units were opened most recently so you don't have to walk back to the warehouse for every lookup.
- **Opening hook:** Disks don't know about rows, tables, or users. They just know about *bytes at offsets*. The storage engine's job is to translate the messy world of variable-sized rows into the rigid world of fixed-size blocks.
- **Key insight:** Databases use **fixed-size pages** because disks move data in fixed-size blocks. Picking a page size that matches the disk's native block size is why databases feel fast.
- **"Why should I care?":** When someone says "this query is slow because of too many page reads," or when you're choosing a `cacheSize` parameter, you need to know what a page *is*. Knowing the page size also helps you sanity-check AI suggestions — "cache 1000 pages" means ~4MB, not 4GB.

### Code Snippets (pre-extracted)

**File: MiniDatabaseEngine/Storage/Page.cs (lines 6-22)**
```csharp
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
```

**File: MiniDatabaseEngine/Storage/Extent.cs (lines 6-36)**
```csharp
public class Extent
{
    public const int PagesPerExtent = 8;
    
    public int ExtentId { get; set; }
    public Page[] Pages { get; set; }
    public bool IsDirty => Pages.Any(p => p.IsDirty);
    
    public Extent(int extentId)
    {
        ExtentId = extentId;
        Pages = new Page[PagesPerExtent];
        
        for (int i = 0; i < PagesPerExtent; i++)
        {
            int pageId = extentId * PagesPerExtent + i;
            Pages[i] = new Page(pageId);
        }
    }
```

**File: MiniDatabaseEngine/Storage/PageCache.cs (lines 36-59) — LRU put**
```csharp
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
                _cache.TryRemove(removed.Page.PageId, out _);
        }
    }
}
```

### Interactive Elements

- **Hero visual — pattern cards (4 cards):**
  1. **Page — 4096 bytes.** "The atom of storage. Everything on disk is a page."
  2. **Extent — 8 pages = 32 KB.** "A neighborhood of pages. Fetched as a group for locality."
  3. **Dirty flag.** "One bit that says 'this page has changes that haven't hit disk yet.'"
  4. **LRU cache.** "Keeps hot pages in RAM. Evicts the one nobody touched in the longest time."
- **Code↔English translation #1** — Page.cs. English explains: fixed size, PageId used for disk offset math, Data is the raw bytes, IsDirty means "needs writing."
- **Code↔English translation #2** — Extent.cs constructor. English explains the `extentId * PagesPerExtent + i` math visually (page 17 lives in extent 2, slot 1).
- **Code↔English translation #3** — PageCache.Put (LRU). English explains head/tail metaphor and eviction.
- **Visual diagram (custom HTML with CSS grid)** — a grid showing extents as rows, each extent as 8 colored cells. Highlight page 17 → show extent 2, offset 1. Include a byte-offset readout: `file offset = pageId × 4096`.
- **Drag-and-drop quiz** — Items: `Page 0`, `Page 7`, `Page 8`, `Page 15`, `Page 23`. Zones: `Extent 0`, `Extent 1`, `Extent 2`. Tests the math.
- **Callout (info):** "**Why 4KB?** Most SSDs and OS page caches work in 4KB blocks. Matching the page size to the OS block size means one database page = one disk I/O — no wasted reads."
- **Callout (accent):** "**Dirty pages are lazy.** A page can be modified in memory dozens of times and only written to disk once, when something asks for a flush. That's why databases are fast — disk writes are *batched*."
- **Quiz (3 Qs):**
  1. *Calculation:* If the cache holds 100 pages at 4KB each, how much RAM? (Answer: ~400 KB. A common misread is "4 MB" or "400 MB" — watch the units.)
  2. *Scenario:* You've written 10,000 rows but haven't called `Flush()`. Power cut. What survives? (Answer: Whatever made it into the WAL — the pages in memory are gone. Teaser for module 5.)
  3. *Architecture:* Why does the cache evict the **least** recently used page, not the least frequently used? (Answer: Recency is cheap to track — one pointer move. Frequency requires counting every access. Cheap approximation wins.)

### Reference Files to Read
- `references/content-philosophy.md`
- `references/gotchas.md`
- `references/interactive-elements.md` → "Code ↔ English Translation Blocks", "Pattern/Feature Cards", "Drag-and-Drop Matching", "Callout Boxes", "Multiple-Choice Quizzes", "Glossary Tooltips"

### Tooltips
`page`, `extent`, `byte`, `4 KB`, `disk`, `offset`, `cache`, `LRU`, `dirty flag`, `flush`, `memory-mapped file` (just one line), `serialize`, `doubly-linked list`.

### Connections
- **Previous module:** Module 1 introduced all 5 layers. This module zooms into the *storage* layer at the bottom.
- **Next module:** Module 3, "The B+ Tree." Once you know how bytes are stored, the next question is: *how do we find one specific row among millions?*
- **Tone/style notes:** Teal accent. Keep the warehouse/self-storage metaphor consistent through the module — don't switch metaphors partway. Use `--color-actor-1` (warm peach) for pages, `--color-actor-2` (mustard) for extents in any custom diagrams.
