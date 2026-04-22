# Module 7: Observability — Watching the Engine Run

### Teaching Arc
- **Metaphor:** The **cockpit instrument panel of a plane**. The pilot can't open the hood and look at the engine mid-flight — they rely on gauges (metrics) and the cockpit voice recorder (structured logs) to understand what's happening. Without instruments, you're flying blind and only learn there's a problem when the plane falls.
- **Opening hook:** Your database has been running for three days. Was everything fine? Did commits happen? How many? How many rolled back? If you can't answer those questions in under five seconds, your system isn't observable.
- **Key insight:** Observability has two halves — **metrics** (numbers that count events over time, cheap to collect) and **structured logs** (rich, searchable event records, expensive but detailed). You need both. Metrics tell you *something is wrong*; logs tell you *what*.
- **"Why should I care?":** When AI writes `Console.WriteLine("did the thing")`, that's not observability — that's debug-print pollution. Recognizing the difference between *structured* logging and *print-debugging* is one of the biggest quality markers in professional code.

### Code Snippets (pre-extracted)

**File: MiniDatabaseEngine/Observability.cs (lines 13-38) — structured log entry**
```csharp
public sealed class DatabaseLogEntry
{
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
    public DatabaseLogLevel Level { get; init; }
    public string EventName { get; init; } = string.Empty;
    public string Message { get; init; } = string.Empty;
    public IReadOnlyDictionary<string, object?> Properties { get; init; } = new Dictionary<string, object?>();

    public string ToJson()
    {
        var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["timestamp"] = Timestamp.ToString("O"),
            ["level"] = Level.ToString(),
            ["event"] = EventName,
            ["message"] = Message
        };

        foreach (var pair in Properties)
        {
            payload[pair.Key] = pair.Value;
        }

        return JsonSerializer.Serialize(payload);
    }
}
```

**File: MiniDatabaseEngine/Observability.cs (lines 45-54) — the JSON console logger**
```csharp
public sealed class JsonConsoleDatabaseLogger : IDatabaseLogger
{
    public void Log(DatabaseLogEntry entry)
    {
        if (entry == null)
            throw new ArgumentNullException(nameof(entry));

        Console.WriteLine(entry.ToJson());
    }
}
```

**File: MiniDatabaseEngine/Observability.cs (lines 82-119) — thread-safe metrics**
```csharp
internal sealed class DatabaseMetrics
{
    private long _tablesCreated;
    private long _transactionsStarted;
    private long _inserts;
    private long _updates;
    private long _deletes;
    private long _flushes;
    private long _checkpoints;
    private long _backupsCreated;
    private long _integrityChecks;

    public void IncrementTablesCreated() => Interlocked.Increment(ref _tablesCreated);
    public void IncrementTransactionsStarted() => Interlocked.Increment(ref _transactionsStarted);
    public void IncrementInserts() => Interlocked.Increment(ref _inserts);
    public void IncrementUpdates() => Interlocked.Increment(ref _updates);
    public void IncrementDeletes() => Interlocked.Increment(ref _deletes);
    public void IncrementFlushes() => Interlocked.Increment(ref _flushes);
    public void IncrementCheckpoints() => Interlocked.Increment(ref _checkpoints);
    public void IncrementBackupsCreated() => Interlocked.Increment(ref _backupsCreated);
    public void IncrementIntegrityChecks() => Interlocked.Increment(ref _integrityChecks);

    public DatabaseMetricsSnapshot Snapshot()
    {
        return new DatabaseMetricsSnapshot
        {
            TablesCreated = Interlocked.Read(ref _tablesCreated),
            TransactionsStarted = Interlocked.Read(ref _transactionsStarted),
            Inserts = Interlocked.Read(ref _inserts),
            Updates = Interlocked.Read(ref _updates),
            Deletes = Interlocked.Read(ref _deletes),
            Flushes = Interlocked.Read(ref _flushes),
            Checkpoints = Interlocked.Read(ref _checkpoints),
            BackupsCreated = Interlocked.Read(ref _backupsCreated),
            IntegrityChecks = Interlocked.Read(ref _integrityChecks)
        };
    }
}
```

**File: README.md — how a user uses it**
```csharp
using var db = new Database(
    "mydata.mde",
    options: new DatabaseOptions
    {
        Logger = new JsonConsoleDatabaseLogger(),
        EnableMetrics = true
    });

var metrics = db.GetMetricsSnapshot();
Console.WriteLine($"Inserts: {metrics.Inserts}, Checkpoints: {metrics.Checkpoints}");
```

### Interactive Elements

- **Hero visual — side-by-side "bad vs good logging":**
  - Left (bad): `Console.WriteLine("insert done");` — no timestamp, no context, not searchable.
  - Right (good): `{"timestamp":"2026-04-22T10:12:03Z","level":"Information","event":"row.inserted","table":"Users","txn":42}` — structured, greppable, filterable.
  Render as a two-column `translation-block`-like side-by-side (not a translation, just a comparison).
- **Code↔English translation #1** — `DatabaseLogEntry` + `ToJson`. Call out: timestamp as ISO string, event name as the *stable* key (search for `event:row.inserted`), properties as key-value context.
- **Code↔English translation #2** — the metrics counter class. Focus on `Interlocked.Increment` — the one thread-safety primitive that makes counters safe without locks.
- **Code↔English translation #3** — the consumer side (README snippet). English: "register a logger, flip a flag, read a snapshot — three lines and you can SEE what the engine is doing."
- **Pattern cards — metrics catalog (9 cards, one per counter):** `TablesCreated`, `TransactionsStarted`, `Inserts`, `Updates`, `Deletes`, `Flushes`, `Checkpoints`, `BackupsCreated`, `IntegrityChecks`. Each card: name, icon, one-liner ("Count of commit-triggered flushes — rises when the WAL is actively pushing data to disk").
- **Badge list — events emitted by the engine (7 badges):**
  - `database.opened` — db file opened
  - `table.created` — new table + column count
  - `transaction.started` — begin txn
  - `row.inserted` / `row.updated` / `row.deleted` — data modifications
  - `database.checkpoint` — checkpoint completed
  - `database.flushed` — pages flushed
  - `database.integrity_check` — health check with issue count
- **Message flow animation — a write through the telemetry pipeline** — 4 actors: User Code, Database, Logger, Metrics Counter. Steps:
  1. User calls Insert
  2. Database calls `IncrementInserts` (cheap, atomic)
  3. Database emits `row.inserted` log entry
  4. Logger serializes to JSON and writes
  5. Later: User calls `GetMetricsSnapshot`
  6. Snapshot returns atomic read of all counters
  Apostrophe-free labels.
- **Callout (accent):** "**Metrics tell you *what*; logs tell you *why*.** A metrics dashboard showing 'commits dropped 80%' is an alert. The matching log entries show which transactions failed and how — that's the root cause."
- **Callout (info):** "**Interlocked.Increment is lock-free.** It uses a CPU instruction (compare-and-swap) that's atomic at the hardware level. That's why you can count millions of events per second without contention."
- **Quiz (4 Qs):**
  1. *Scenario:* "You suspect a query is causing storage to blow up. Which single metric would you watch live?" (Answer: `Flushes` or `Checkpoints` — both indicate disk write pressure.)
  2. *Debugging:* "Your boss wants to know 'were any transactions rolled back last hour?'" Can this engine answer it? (Answer: Only partially — `TransactionsStarted` is tracked but there's no explicit `TransactionsRolledBack` counter. You'd have to add one or infer from logs.)
  3. *Architecture:* "Why not just use `Console.WriteLine` everywhere for logs?" (Answer: You can't filter, can't ship to Splunk/Datadog, can't grep by event name, can't associate properties. Structured logs unlock all of those.)
  4. *Scenario:* "Two threads both call `IncrementInserts` at the same instant. Can the counter lose an increment?" (Answer: No — `Interlocked.Increment` is atomic. A plain `_inserts++` would lose updates.)

### Reference Files to Read
- `references/content-philosophy.md`
- `references/gotchas.md`
- `references/interactive-elements.md` → "Code ↔ English Translation Blocks", "Pattern/Feature Cards", "Permission/Config Badges", "Message Flow / Data Flow Animation", "Callout Boxes", "Multiple-Choice Quizzes", "Glossary Tooltips"

### Outro
The module (and the course) ends with a short **"Where to go next"** card cluster — 3 cards:
- **Read the real databases.** SQLite's source is famously readable. Postgres's WAL code is legendary.
- **Instrument your own code.** Start with one counter and one structured log line per boundary (API → DB, DB → disk).
- **Question every AI suggestion.** Now you have the vocabulary to push back with specifics: "which layer does this belong in?", "how does this affect the WAL?", "what's the lock order?"

### Tooltips
`observability`, `metrics`, `structured logging`, `log level`, `JSON`, `ISO timestamp`, `counter`, `gauge`, `Interlocked`, `atomic`, `snapshot`, `dashboard`, `grep`, `event`, `telemetry`.

### Connections
- **Previous module:** Module 6 made the engine safe under concurrent access. This module makes it *visible* while it runs.
- **Next module:** None — this is the final module.
- **Tone/style notes:** Teal accent. Cockpit/instrument metaphor through the whole module. End warmly — acknowledge the learner finished seven dense modules, and make clear the goal was never to become a database engineer, it was to become *fluent enough to steer AI and debug production*.
