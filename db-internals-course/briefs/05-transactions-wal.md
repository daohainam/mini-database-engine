# Module 5: Transactions, WAL & Crash Recovery

### Teaching Arc
- **Metaphor:** An **airline check-in desk**. You present your tickets (transaction begins). The agent writes every item into an official logbook *before* stamping anything on your boarding passes (Write-Ahead Log). If the power cuts mid-check-in, the next agent in the morning reads the logbook and finishes exactly the check-ins that were marked "complete." Anything without a complete stamp is thrown out. That's how airlines avoid losing your luggage — and how databases avoid losing your money.
- **Opening hook:** A bank transfer. You debit account A, then credit account B. What if the lights go out between those two steps? Without transactions, account A is poorer and account B is no richer. Money just evaporates.
- **Key insight:** **Durability requires logging before doing.** Applying the change first and hoping to log it later is a trap — if you crash between the two, the change is done but the log doesn't know. The invariant is: "Log first, apply after." This one rule powers crash recovery everywhere.
- **"Why should I care?":** When AI writes "we'll save the order and send the email in parallel" — that's wrong. When AI suggests `cache.set()` before `db.commit()` — that's wrong. Recognizing the "log first" pattern lets you catch real bugs in AI code instantly.

### Code Snippets (pre-extracted)

**File: MiniDatabaseEngine/Transaction/WALEntry.cs (lines 20-40) — WALEntry shape**
```csharp
public class WALEntry
{
    private const int MaxCheckpointActiveTransactionCount = 100_000;
    private const int MaxStringByteLength = 64 * 1024;
    private const int MaxValueLength = 1024 * 1024;

    public long TransactionId { get; set; }
    public WALOperationType OperationType { get; set; }
    public string TableName { get; set; } = string.Empty;
    public object? Key { get; set; }
    public byte[]? OldValue { get; set; }
    public byte[]? NewValue { get; set; }
    public long Timestamp { get; set; }
    public long SequenceNumber { get; set; }
    public List<long> CheckpointActiveTransactionIds { get; set; } = new();
    public long CheckpointNextTransactionId { get; set; }
}
```

**File: MiniDatabaseEngine/Transaction/Transaction.cs (lines 57-85) — Commit**
```csharp
public void Commit()
{
    _lock.EnterWriteLock();
    try
    {
        if (_state != TransactionState.Active)
            throw new InvalidOperationException($"Cannot commit transaction in state: {_state}");

        // Log the commit
        _walManager.AppendEntry(new WALEntry
        {
            TransactionId = _transactionId,
            OperationType = WALOperationType.Commit
        });

        // Force flush to ensure durability
        _walManager.Flush();

        // Apply buffered writes only after WAL commit is durable.
        _commitApplyCallback(_entries);

        _state = TransactionState.Committed;
        _transactionManager.CompleteTransaction(_transactionId);
    }
    finally
    {
        _lock.ExitWriteLock();
    }
}
```

**File: MiniDatabaseEngine/Transaction/Transaction.cs (lines 119-132) — LogInsert**
```csharp
internal void LogInsert(string tableName, object key, byte[] value)
{
    EnsureActive();
    var entry = new WALEntry
    {
        TransactionId = _transactionId,
        OperationType = WALOperationType.Insert,
        TableName = tableName,
        Key = key,
        NewValue = value
    };
    _entries.Add(entry);
    _walManager.AppendEntry(entry);
}
```

**File: MiniDatabaseEngine/Transaction/TransactionManager.cs (lines 96-140) — Recovery (trimmed view for size; agent: keep as-is)**
```csharp
public void RecoverFromWAL(Action<WALEntry> applyEntry)
{
    _lock.EnterWriteLock();
    try
    {
        var entries = _walManager.ReadEntriesForRecovery();
        var transactions = new Dictionary<long, List<WALEntry>>();
        var committedTransactions = new HashSet<long>();

        foreach (var entry in entries)
        {
            if (entry.OperationType == WALOperationType.Checkpoint) continue;

            if (entry.OperationType == WALOperationType.Commit)
                committedTransactions.Add(entry.TransactionId);
            else if (entry.OperationType == WALOperationType.Rollback)
                continue;
            else if (entry.OperationType != WALOperationType.BeginTransaction)
            {
                if (!transactions.ContainsKey(entry.TransactionId))
                    transactions[entry.TransactionId] = new List<WALEntry>();
                transactions[entry.TransactionId].Add(entry);
            }
        }

        foreach (var txnId in committedTransactions)
        {
            if (transactions.TryGetValue(txnId, out var txnEntries))
            {
                foreach (var entry in txnEntries)
                    applyEntry(entry);
            }
        }
    }
    finally { _lock.ExitWriteLock(); }
}
```

### Interactive Elements

- **Hero visual — ACID pattern cards (4 cards):**
  1. **Atomicity** — "All or nothing. Never half-done."
  2. **Consistency** — "Invariants (like 'total balance doesn't change') hold before and after."
  3. **Isolation** — "Concurrent transactions look like they ran one at a time."
  4. **Durability** — "Committed means committed, even if the server explodes."
  Use `--color-actor-1..4` for the left borders.
- **Group Chat Animation (required)** — chat id `chat-wal`, actors: User, Transaction, WAL, BPlusTree. Messages (apostrophe-free):
  1. User → Transaction: "begin"
  2. Transaction → WAL: "BEGIN txn 42"
  3. User → Transaction: "insert row Alice"
  4. Transaction → WAL: "append INSERT entry for txn 42"
  5. User → Transaction: "commit"
  6. Transaction → WAL: "append COMMIT marker, then FLUSH to disk"
  7. WAL → Transaction: "durable on disk — safe"
  8. Transaction → BPlusTree: "now apply the buffered INSERT to the index"
  9. Transaction → User: "committed"
- **Code↔English translation #1** — WALEntry fields. Emphasize: `OldValue` + `NewValue` together let us redo *or* undo. SequenceNumber gives a total order. CheckpointActiveTransactionIds is covered below.
- **Code↔English translation #2** — Commit method. Critical lines to highlight: "append commit entry," "flush the WAL" (durability happens here), "then apply to the index." Call out "flush before apply" as the ACID-defining moment.
- **Code↔English translation #3** — RecoverFromWAL. English: "group by transaction id, keep only the ones that have a Commit, replay just those — everything else is thrown away."
- **Data flow animation — Crash Recovery** — `flow-animation` with 4 actors: Disk/WAL file, Recovery Reader, Transaction Buckets, B+Tree. Steps:
  1. Process starts. Look for a WAL file.
  2. Read every entry into memory.
  3. Bucket entries by transaction id.
  4. Find committed buckets.
  5. Replay committed inserts/updates/deletes onto the B+Tree.
  6. Uncommitted transactions are dropped.
  7. Done. Database resumes normal operation.
- **Numbered step cards — Checkpoint explained:** 4 steps — "Flush all dirty pages" → "Write a Checkpoint WAL entry with active txns" → "Truncate old WAL" → "Smaller WAL = faster recovery next time."
- **Callout (accent):** "**'Log first, apply after' is ACID's beating heart.** If the commit record is on disk, recovery will replay the change. If not, it never happened. There's no third state."
- **Callout (warning):** "**Deferred writes.** This engine does NOT apply changes to the B+Tree until after commit. That makes rollback almost free — there's nothing to undo. The tradeoff: other transactions can't see your in-flight writes."
- **Scenario quiz (4 Qs):**
  1. *Scenario:* "Power cuts *after* the commit record was written but *before* the index was updated. On restart, is the row there?" (Answer: Yes — recovery replays committed entries onto the index. The WAL is the source of truth.)
  2. *Scenario:* "Power cuts *between* two inserts in the same transaction, before commit." (Answer: Nothing committed → both inserts discarded.)
  3. *Debugging:* "A stress test shows committed data lost after a crash." Where to look first? (Answer: Is `_walManager.Flush()` being called before the apply callback? Durability relies on that flush.)
  4. *Architecture:* "Why doesn't rollback need to 'undo' anything?" (Answer: Because of deferred writes — changes only land in the index after commit. Rollback just discards the buffer.)

### Reference Files to Read
- `references/content-philosophy.md`
- `references/gotchas.md`
- `references/interactive-elements.md` → "Code ↔ English Translation Blocks", "Group Chat Animation", "Message Flow / Data Flow Animation", "Pattern/Feature Cards", "Numbered Step Cards", "Callout Boxes", "Multiple-Choice Quizzes", "Scenario Quiz", "Glossary Tooltips"

### Tooltips
`transaction`, `ACID`, `atomicity`, `consistency`, `isolation`, `durability`, `WAL`, `write-ahead log`, `commit`, `rollback`, `checkpoint`, `flush`, `fsync`, `deferred write`, `redo`, `undo`, `idempotent`, `checksum`.

### Connections
- **Previous module:** Module 4 showed how reads work. This module is about writes that must *survive*.
- **Next module:** Module 6, "Concurrency." Multiple transactions at once — how does the engine keep them from stepping on each other?
- **Tone/style notes:** Teal accent. Keep the airline/logbook metaphor throughout — don't reach for "ledger" (overused). When using the word "log," always pair it with "durable" on first mention so the learner builds the right mental model.
