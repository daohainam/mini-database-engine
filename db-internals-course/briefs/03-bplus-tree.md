# Module 3: The B+ Tree — Finding a Needle Fast

### Teaching Arc
- **Metaphor:** A **phone book with an index tab on the edge**. The tabs (internal nodes) don't contain phone numbers — they just tell you which section to flip to. Phone numbers (values) live only at the alphabetized pages at the end (leaf nodes), which are stapled in order so you can flip to "Smith" and just keep reading forward to "Thompson, Turner, Vance" without going back through the tabs.
- **Opening hook:** Scanning 10 million rows to find user #47,392,185 would take forever. A B+ tree turns that search into about 23 comparisons — like binary-searching a physical book, one page at a time.
- **Key insight:** A B+ tree gives you **O(log n) lookups** AND **fast range scans** from a single data structure. That's why every major database — Postgres, SQL Server, SQLite, MySQL — uses it for primary-key indexes. Not a simple binary tree, not a hash table, not a B-tree (no plus). A B+ tree.
- **"Why should I care?":** When AI suggests "just add an index," you need to understand the cost: indexes aren't free — they slow down writes (every insert updates the tree) to speed up reads. Knowing the shape of the tree helps you reason about that trade-off.

### Code Snippets (pre-extracted)

**File: MiniDatabaseEngine/BPlusTree/BPlusTreeNode.cs (lines 21-66)**
```csharp
public abstract class BPlusTreeNode
{
    public bool IsLeaf { get; protected set; }
    public BPlusTreeNode? Parent { get; set; }
    public List<object> Keys { get; protected set; }
    
    protected BPlusTreeNode(bool isLeaf)
    {
        IsLeaf = isLeaf;
        Keys = new List<object>();
    }
    
    public abstract int KeyCount { get; }
}

public class BPlusTreeInternalNode : BPlusTreeNode
{
    public List<BPlusTreeNode> Children { get; private set; }
    
    public BPlusTreeInternalNode() : base(false)
    {
        Children = new List<BPlusTreeNode>();
    }
    
    public override int KeyCount => Keys.Count;
}

public class BPlusTreeLeafNode : BPlusTreeNode
{
    public List<object?> Values { get; private set; }
    public BPlusTreeLeafNode? Next { get; set; }
    public BPlusTreeLeafNode? Previous { get; set; }
    
    public BPlusTreeLeafNode() : base(true)
    {
        Values = new List<object?>();
    }
    
    public override int KeyCount => Keys.Count;
}
```

**File: MiniDatabaseEngine/BPlusTree/BPlusTree.cs (lines 32-44) — Insert**
```csharp
public void Insert(object key, object? value)
{
    lock (_lockObject)
    {
        var leaf = FindLeafNode(key);
        InsertIntoLeaf(leaf, key, value);
        
        if (leaf.KeyCount > _order - 1)
        {
            SplitLeafNode(leaf);
        }
    }
}
```

**File: MiniDatabaseEngine/BPlusTree/BPlusTree.cs (lines 49-63) — Search**
```csharp
public object? Search(object key)
{
    lock (_lockObject)
    {
        var leaf = FindLeafNode(key);
        int index = FindKeyIndex(leaf.Keys, key);
        
        if (index < leaf.Keys.Count && _comparer.Compare(leaf.Keys[index], key) == 0)
        {
            return ((BPlusTreeLeafNode)leaf).Values[index];
        }
        
        return null;
    }
}
```

**File: MiniDatabaseEngine/BPlusTree/BPlusTree.cs (lines 120-147) — Range scan**
```csharp
public IEnumerable<KeyValuePair<object, object?>> Range(object? minKey, object? maxKey)
{
    lock (_lockObject)
    {
        var results = new List<KeyValuePair<object, object?>>();
        var leaf = minKey != null ? FindLeafNode(minKey) : GetFirstLeaf();
        
        while (leaf != null)
        {
            for (int i = 0; i < leaf.Keys.Count; i++)
            {
                var key = leaf.Keys[i];
                
                if (minKey != null && _comparer.Compare(key, minKey) < 0)
                    continue;
                    
                if (maxKey != null && _comparer.Compare(key, maxKey) > 0)
                    return results;
                    
                results.Add(new KeyValuePair<object, object?>(key, leaf.Values[i]));
            }
            
            leaf = leaf.Next;
        }
        
        return results;
    }
}
```

### Interactive Elements

- **Hero visual — ASCII-style tree diagram** using CSS grid / flex. Show:
  ```
        [ 30 | 60 ]          <- internal
       /     |     \
   [5,15,25] [35,45] [65,75,90]   <- leaves ↔ linked ↔
  ```
  with dashed arrows between leaves indicating the `Next`/`Previous` pointers. Use accent teal for the tree, warm orange arrows for leaf links.
- **Code↔English translation #1** — BPlusTreeNode.cs abstract + leaf class. English: "Every node has keys. Leaves also have values AND pointers to siblings. Internal nodes have children but no values."
- **Code↔English translation #2** — Search method. Line-by-line: lock, walk down to the right leaf, binary-search inside the leaf, confirm match.
- **Code↔English translation #3** — Range method. English emphasizes the "walk the leaf chain" trick — this is why B+ trees beat plain B-trees for range queries.
- **Pattern cards (3) — Tree Operations:**
  1. **Search — O(log n).** "Climb down the tabs to the right page."
  2. **Insert + Split.** "When a leaf overflows, cut it in half and promote the middle key to the parent — the tree grows upward, not downward."
  3. **Range scan.** "Find the start, then walk sideways through the stapled leaves."
- **"Build the Tree" visualization (static diagram sequence)** — Show 4 snapshots: insert 10, insert 20, insert 30, insert 40 (with order=3, so splits happen). Visualize how splits bubble up. Use numbered-step-cards layout with one SVG/CSS diagram per card.
- **Callout (accent):** "**Why B+ (not B)?** In a plain B-tree, values live at every level. In a B+ tree, values live only in leaves, and leaves are linked. That makes range scans — `WHERE age BETWEEN 20 AND 30` — a straight walk instead of a full traversal."
- **Callout (info):** "**Fanout = how wide each node is.** This tree uses order-4 nodes (3 keys, 4 children). Real databases use order-100+. Higher fanout = shallower tree = fewer disk reads."
- **Quiz (4 Qs):**
  1. *Scenario:* "Find all users with ID between 1000 and 2000." Which B+ tree operation runs? (Answer: `Range` — and the cost is "find leaf for 1000" + "walk forward until key > 2000." Neither full scan nor one-at-a-time.)
  2. *Debugging:* After many inserts the tree has become very tall and queries feel slow. What changed? (Answer: Lots of splits; a higher tree means more comparisons per search. A real DB would rebalance; this one still works but gets slower.)
  3. *Architecture:* Why do leaves have `Previous` AND `Next` pointers when the code only uses `Next` for range scans? (Answer: It's future-proofing for reverse iteration / ORDER BY DESC.)
  4. *Tracing:* Insert order `[5, 15, 25, 35]` into an order-3 tree (max 2 keys per leaf). What's the root after the last insert? (Answer: Root is internal, with key `15`; left leaf `[5]`, right leaf `[15, 25, 35]` → overflow triggers another split. Walk through the cascading split.)

### Reference Files to Read
- `references/content-philosophy.md`
- `references/gotchas.md`
- `references/interactive-elements.md` → "Code ↔ English Translation Blocks", "Pattern/Feature Cards", "Numbered Step Cards", "Callout Boxes", "Multiple-Choice Quizzes", "Glossary Tooltips"

### Tooltips
`B+ tree`, `leaf node`, `internal node`, `fanout`, `order (of a tree)`, `O(log n)`, `split`, `promote`, `linked list`, `doubly linked`, `binary search`, `comparer`, `IEnumerable`, `yield`.

### Connections
- **Previous module:** Module 2 showed where bytes live. This module shows how we *find* bytes fast.
- **Next module:** Module 4, "Indexes & Query Plans." Now that you know the tree exists, you'll see how the LINQ provider decides when to *use* it vs. fall back to a full scan.
- **Tone/style notes:** Teal accent. Every diagram should use `--font-mono` for node contents. Keep the phone-book metaphor consistent; never use restaurant/library/warehouse.
