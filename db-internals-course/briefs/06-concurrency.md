# Module 6: Concurrency — Many Hands, One Ledger

### Teaching Arc
- **Metaphor:** A **revolving door at a subway station**. Many people can walk through *at the same time* in the same direction (many readers) — but if one person wants to stop and tie their shoelaces in the middle (a writer), everyone else must wait until they're done. A poorly designed door either lets conflicting people crash into each other (corrupted data) or blocks everyone for every single person (deadlock and slowness). The database's lock scheme is its revolving-door design.
- **Opening hook:** Two users click "Buy" on the last ticket at the exact same millisecond. Who gets it? If the database messes up this question even once, the business loses trust. Concurrency control is the invisible layer that answers it billions of times a day.
- **Key insight:** **Reader-writer locks** let you have your cake (many parallel reads) and eat it (exclusive writes), as long as writes are rare compared to reads. And **strict lock ordering** prevents the deadly embrace called *deadlock* — where thread A waits on B while B waits on A.
- **"Why should I care?":** Concurrency bugs are the hardest to reproduce and the most dangerous in production. When AI writes `lock(something)` at random places, you need to recognize the smell. Knowing the lock order of your system is like knowing the emergency exits — rare to need, priceless when you do.

### Code Snippets (pre-extracted)

**File: MiniDatabaseEngine/Storage/StorageEngine.cs (lines 8-30) — Lock ordering contract**
```csharp
/// <summary>
/// Thread Safety:
/// This class uses ReaderWriterLockSlim to ensure thread-safe operations.
/// Multiple threads can read concurrently, but write operations are exclusive.
/// 
/// Lock Ordering (to prevent deadlocks when nested locking occurs):
/// When multiple locks need to be acquired, always acquire them in this order:
/// 1. Database._lock (schema-level operations)
/// 2. Table._lock (table-level operations)
/// 3. BPlusTree._lockObject (index operations)
/// 4. StorageEngine._lock (this class - storage operations)
/// 5. PageCache/ExtentCache locks (cache operations)
/// </summary>
public class StorageEngine : IDisposable
{
    private readonly ReaderWriterLockSlim _lock;
```

**File: MiniDatabaseEngine/Table.cs (lines 54-85) — Write lock in Insert**
```csharp
public void Insert(DataRow row, Transaction.Transaction? transaction = null)
{
    _lock.EnterWriteLock();
    try
    {
        var key = GetPrimaryKey(row);
        ValidateRowForWrite(row);

        var keyExists = _index.Search(key) != null;
        var keyPendingInTransaction = transaction?.HasBufferedValueForKey(_schema.TableName, key, GetPrimaryKeyDataType()) ?? false;
        if (keyExists || keyPendingInTransaction)
            throw new InvalidOperationException($"Duplicate primary key value '{key}'.");

        var serialized = SerializeRow(row);

        if (transaction != null)
        {
            transaction.LogInsert(_schema.TableName, key, serialized);
            _nextRowId++;
            return;
        }

        _index.Insert(key, serialized);
        _nextRowId++;
    }
    finally
    {
        _lock.ExitWriteLock();
    }
}
```

**File: MiniDatabaseEngine/Table.cs (lines 226-241) — Read lock in SelectByKey**
```csharp
public DataRow? SelectByKey(object key)
{
    _lock.EnterReadLock();
    try
    {
        var serialized = _index.Search(key);
        if (serialized == null)
            return null;
        
        return DeserializeRow((byte[])serialized);
    }
    finally
    {
        _lock.ExitReadLock();
    }
}
```

**File: MiniDatabaseEngine/BPlusTree/BPlusTree.cs (lines 32-44) — Simple lock on the tree**
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

### Interactive Elements

- **Hero visual — "Reader-Writer Lock" state diagram:**
  - Three circles: `IDLE`, `READERS (N)`, `WRITER (1)`.
  - Arrows: IDLE→READERS (any number of readers can enter), IDLE→WRITER (a single writer enters), READERS→IDLE (last reader leaves), WRITER→IDLE (writer finishes). No arrows between READERS and WRITER.
  - Implement as CSS-styled `icon-rows` + arrows with `font-mono` labels.
- **Icon-label rows — the 5-level lock stack:**
  - 🔐 Database lock — schema-level (e.g., create table)
  - 🔒 Table lock — per-table read/write
  - 🔑 B+ Tree lock — tree structural changes
  - 🗝️ StorageEngine lock — page-level I/O
  - 📎 PageCache / ExtentCache locks — LRU list mutations
  Each row shows the depth level and what it guards.
- **Code↔English translation #1** — the lock-ordering XML doc block from StorageEngine.cs. English explains: "picking up locks in this order is a contract. Two threads that both follow it can never deadlock."
- **Code↔English translation #2** — Table.Insert with write lock. Focus on the `try/finally` — locks MUST be released even if exceptions fly.
- **Code↔English translation #3** — side-by-side: SelectByKey (EnterReadLock) vs Insert (EnterWriteLock). Make the difference obvious.
- **Group chat animation — "deadlock almost happens"** — chat id `chat-locks`, actors: Thread-A, Thread-B, Lock-A, Lock-B. Messages:
  1. Thread-A → Lock-A: "I have you"
  2. Thread-B → Lock-B: "I have you"
  3. Thread-A → Lock-B: "now give me you too"
  4. Lock-B → Thread-A: "I am held by B"
  5. Thread-B → Lock-A: "I need you too"
  6. Lock-A → Thread-B: "I am held by A"
  7. (narrator bubble) → both: "nobody moves. deadlock."
  8. (narrator bubble) → both: "fix: always acquire locks in the same global order."
- **Spot-the-bug challenge** — Show a method that holds two locks in the "wrong" order:
  ```csharp
  public void Weird()
  {
      _cache.Lock.EnterWriteLock();          // level 5
      try
      {
          _storage.Lock.EnterWriteLock();    // level 4 — WRONG, acquired after 5
          // ...
      }
      finally { _cache.Lock.ExitWriteLock(); }
  }
  ```
  The buggy line is the second `EnterWriteLock`. Reveal: "This violates lock ordering — a sibling thread holding the storage lock could be waiting for the cache lock, and we'd be stuck."
- **Callout (accent):** "**Lock ordering is a design discipline, not a runtime check.** The compiler won't stop you from violating it — only the review discipline will. Every new method that takes two locks is a potential deadlock until proven otherwise."
- **Callout (info):** "**Coarse vs fine-grained locking.** This engine locks whole tables at a time — simple, but means one writer blocks all readers of that table. Real databases use row-level or page-level locks to allow more parallelism, at the cost of much more complex code."
- **Quiz (4 Qs):**
  1. *Scenario:* "Reads are fast but writes sometimes stall for seconds." What's likely happening? (Answer: A long-running writer holds the table's write lock, starving readers. ReaderWriterLockSlim grants the writer exclusive access.)
  2. *Debugging:* "The app hangs forever when two threads call `Method1` and `Method2` at the same time." What's the first thing to check? (Answer: Whether the two methods acquire locks in different orders.)
  3. *Architecture:* "Why does `BPlusTree` use a simple `lock(obj)` instead of ReaderWriterLockSlim?" (Answer: Tree operations change structure (splits, re-linking). Allowing concurrent reads while structure changes is dangerous; simple exclusive locking is correct and simpler.)
  4. *Scenario:* "You're editing a row inside a transaction. Another thread calls `SelectAll()`. What happens?" (Answer: With deferred writes, the other thread sees the pre-transaction state. Your buffered change isn't in the index yet — it only lands on commit.)

### Reference Files to Read
- `references/content-philosophy.md`
- `references/gotchas.md`
- `references/interactive-elements.md` → "Code ↔ English Translation Blocks", "Group Chat Animation", "Icon-Label Rows", "Spot the Bug Challenge", "Callout Boxes", "Multiple-Choice Quizzes", "Glossary Tooltips"

### Tooltips
`thread`, `lock`, `mutex`, `reader-writer lock`, `ReaderWriterLockSlim`, `deadlock`, `race condition`, `starvation`, `critical section`, `atomic`, `concurrency`, `parallelism`, `try/finally`, `granularity`, `exclusive`.

### Connections
- **Previous module:** Module 5 explained *what* a transaction is. This module explains *how* many transactions coexist without corrupting each other.
- **Next module:** Module 7, "Observability." Now that you know how it all works, how do you see it working *at runtime*?
- **Tone/style notes:** Teal accent. Subway/revolving-door metaphor — don't switch to "library rules" or similar. Every lock example should release in a `finally` block; any example without one should be called out as wrong.
