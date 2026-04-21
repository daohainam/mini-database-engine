using System.Text.Json;

namespace MiniDatabaseEngine;

public enum DatabaseLogLevel
{
    Debug,
    Information,
    Warning,
    Error
}

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

public interface IDatabaseLogger
{
    void Log(DatabaseLogEntry entry);
}

public sealed class JsonConsoleDatabaseLogger : IDatabaseLogger
{
    public void Log(DatabaseLogEntry entry)
    {
        if (entry == null)
            throw new ArgumentNullException(nameof(entry));

        Console.WriteLine(entry.ToJson());
    }
}

public sealed class DatabaseOptions
{
    public IDatabaseLogger? Logger { get; init; }
    public bool EnableMetrics { get; init; } = true;
}

public sealed class DatabaseMetricsSnapshot
{
    public long TablesCreated { get; init; }
    public long TransactionsStarted { get; init; }
    public long Inserts { get; init; }
    public long Updates { get; init; }
    public long Deletes { get; init; }
    public long Flushes { get; init; }
    public long Checkpoints { get; init; }
    public long BackupsCreated { get; init; }
    public long IntegrityChecks { get; init; }
}

public sealed class DatabaseIntegrityReport
{
    public DateTimeOffset CheckedAtUtc { get; init; } = DateTimeOffset.UtcNow;
    public bool IsHealthy { get; init; }
    public IReadOnlyList<string> Issues { get; init; } = Array.Empty<string>();
}

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

internal sealed class NullDatabaseLogger : IDatabaseLogger
{
    public static NullDatabaseLogger Instance { get; } = new();

    private NullDatabaseLogger()
    {
    }

    public void Log(DatabaseLogEntry entry)
    {
    }
}
