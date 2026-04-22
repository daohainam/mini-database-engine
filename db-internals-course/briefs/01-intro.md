# Module 1: What Actually Happens When You `INSERT` a Row?

### Teaching Arc
- **Metaphor:** A **busy hospital ER** receiving a new patient. The row (patient) passes through triage (Database), a specialist desk (Table), is recorded in the official intake log (WAL) *before* anyone does anything permanent, then filed into the ward roster (B+ Tree), which lives in cabinets (Pages). If the power fails mid-intake, the intake log tells us exactly what was officially admitted.
- **Opening hook:** The user writes one innocent line: `db.Insert("Users", row)`. On the surface it looks trivial — but under the hood, seven layers cooperate in milliseconds to make sure that row is *findable*, *durable*, and *safe from crashes*.
- **Key insight:** A database is not one thing — it is a **stack of specialized layers**, each solving a narrow problem. If you know which layer owns a concern, you know where to look when something goes wrong.
- **"Why should I care?":** When you tell AI "add a column" or "speed up this query," the AI needs to know which layer the change belongs in. If you can name the layers (storage, index, transaction, query), you can steer the AI instead of getting generic, half-right code.

### What this course is about
This is a real C#/.NET 10 embedded database — ~40 source files, around 4,500 lines — that implements the same fundamentals as SQL Server or SQLite in miniature. By the end you'll be able to trace a request from the API call down to the bytes on disk.

### Code Snippets (pre-extracted)

**File: README.md — usage example (public API)**
```csharp
using var db = new Database("mydata.mde", cacheSize: 100, useMemoryMappedFile: false);

var columns = new List<ColumnDefinition>
{
    new ColumnDefinition("Id", DataType.Int, false),
    new ColumnDefinition("Name", DataType.String),
    new ColumnDefinition("Age", DataType.Int)
};

var table = db.CreateTable("Users", columns, primaryKeyColumn: "Id");

var row = new DataRow(table.Schema);
row["Id"] = 1;
row["Name"] = "Alice";
row["Age"] = 30;

db.Insert("Users", row);
```

**File: MiniDatabaseEngine/Table.cs (lines 52-85) — Insert path inside the table**
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

### Interactive Elements

- **Hero visual:** Icon-label rows showing the 5 layers (API / Table / Transaction + WAL / B+ Tree Index / Page-based Storage) with a one-liner for each. Use distinct actor colors.
- **Code↔English translation #1** — the README usage snippet (public API). English explains what a Database/Table/Column/Row is in plain terms.
- **Code↔English translation #2** — the Table.Insert snippet. Walk through: lock, validate, check duplicate, serialize, log-to-WAL OR write-to-index.
- **Data flow animation** — 5 actors (`flow-actor-1` = User, `flow-actor-2` = Database, `flow-actor-3` = Table, `flow-actor-4` = WAL, `flow-actor-5` = B+Tree). Steps JSON:
  - User calls `db.Insert` → Database
  - Database looks up the Table
  - Table validates the row against schema
  - Table serializes row to bytes
  - Serialized bytes appended to the WAL (durability point)
  - Bytes inserted into the B+ Tree by key
  - Returns success to caller
  - Note: use double-quote delimiters if any label has apostrophes; keep labels apostrophe-free to be safe.
- **Numbered step cards** — same 7 steps, shown as a second static read-at-your-own-pace representation.
- **Callout (accent):** "**Separation of concerns** — each layer has one job. Durability is the WAL's job, sort order is the index's job, disk I/O is the storage engine's job. Confusing these is the #1 bug in amateur database code."
- **Quiz (4 Qs):**
  1. *Scenario:* "Users report duplicate-key errors even after rolling back a transaction." Which layer's check might be firing? (Answer: Table's `HasBufferedValueForKey` — in-flight inserts in the same txn also count as duplicates.)
  2. *Architecture:* You want to add compression for stored rows. Which layer should own it? (Answer: The storage/serialization layer — compressing at the API layer bypasses durability guarantees.)
  3. *Debugging:* A write "succeeded" but disappeared after a crash. What's the likely cause? (Answer: The commit didn't flush the WAL to disk before returning — durability was violated.)
  4. *Tracing:* Put these in order: (a) bytes on disk, (b) row serialized to bytes, (c) primary key extracted, (d) WAL entry appended. (Answer: c → b → d → a.)

### Reference Files to Read
- `references/content-philosophy.md` — all sections
- `references/gotchas.md` — checklist
- `references/interactive-elements.md` → "Code ↔ English Translation Blocks", "Message Flow / Data Flow Animation", "Icon-Label Rows", "Numbered Step Cards", "Callout Boxes", "Multiple-Choice Quizzes", "Glossary Tooltips"

### Tooltips (minimum set)
`database engine`, `API`, `primary key`, `schema`, `serialize`, `WAL` (give one-line preview; module 5 covers it deeply), `index`, `B+ tree` (one-liner preview; module 3), `page`, `lock` (one-liner; module 6), `IQueryable`, `LINQ`, `.NET`, `C#`, `namespace`, `nullable reference`.

### Connections
- **Previous module:** (none — this is module 1)
- **Next module:** Module 2, "Pages & Extents — The Storage Floor Plan." Will zoom into the bottom of the stack: how the database physically organizes disk.
- **Tone/style notes:** Accent color = **teal** (`#2A7B9B`). Actors: use `--color-actor-1..5` in order User/Database/Table/WAL/B+Tree. Keep voice warm and concrete — "your row," "your click," not "the caller."
