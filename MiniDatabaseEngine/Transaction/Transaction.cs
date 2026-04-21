namespace MiniDatabaseEngine.Transaction;

/// <summary>
/// Represents the state of a transaction
/// </summary>
public enum TransactionState
{
    Active,
    Committed,
    RolledBack,
    Aborted
}

/// <summary>
/// Represents a database transaction
/// </summary>
public class Transaction : IDisposable
{
    private readonly long _transactionId;
    private readonly WALManager _walManager;
    private readonly TransactionManager _transactionManager;
    private TransactionState _state;
    private readonly ReaderWriterLockSlim _lock;
    private readonly List<WALEntry> _entries;
    private readonly Action<IReadOnlyList<WALEntry>> _commitApplyCallback;

    internal Transaction(
        long transactionId,
        WALManager walManager,
        TransactionManager transactionManager,
        Action<IReadOnlyList<WALEntry>> commitApplyCallback)
    {
        _transactionId = transactionId;
        _walManager = walManager;
        _transactionManager = transactionManager;
        _commitApplyCallback = commitApplyCallback;
        _state = TransactionState.Active;
        _lock = new ReaderWriterLockSlim();
        _entries = new List<WALEntry>();

        // Log the beginning of the transaction
        _walManager.AppendEntry(new WALEntry
        {
            TransactionId = _transactionId,
            OperationType = WALOperationType.BeginTransaction
        });
    }

    public long TransactionId => _transactionId;
    public TransactionState State => _state;

    /// <summary>
    /// Commit the transaction
    /// </summary>
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

    /// <summary>
    /// Rollback the transaction
    /// </summary>
    public void Rollback()
    {
        _lock.EnterWriteLock();
        try
        {
            if (_state != TransactionState.Active)
                throw new InvalidOperationException($"Cannot rollback transaction in state: {_state}");

            // Log the rollback
            _walManager.AppendEntry(new WALEntry
            {
                TransactionId = _transactionId,
                OperationType = WALOperationType.Rollback
            });

            _walManager.Flush();

            _state = TransactionState.RolledBack;
            _transactionManager.CompleteTransaction(_transactionId);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Log an insert operation
    /// </summary>
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

    /// <summary>
    /// Log an update operation
    /// </summary>
    internal void LogUpdate(string tableName, object key, byte[] oldValue, byte[] newValue)
    {
        EnsureActive();
        var entry = new WALEntry
        {
            TransactionId = _transactionId,
            OperationType = WALOperationType.Update,
            TableName = tableName,
            Key = key,
            OldValue = oldValue,
            NewValue = newValue
        };
        _entries.Add(entry);
        _walManager.AppendEntry(entry);
    }

    /// <summary>
    /// Log a delete operation
    /// </summary>
    internal void LogDelete(string tableName, object key, byte[] oldValue)
    {
        EnsureActive();
        var entry = new WALEntry
        {
            TransactionId = _transactionId,
            OperationType = WALOperationType.Delete,
            TableName = tableName,
            Key = key,
            OldValue = oldValue
        };
        _entries.Add(entry);
        _walManager.AppendEntry(entry);
    }

    private void EnsureActive()
    {
        if (_state != TransactionState.Active)
            throw new InvalidOperationException($"Transaction is not active: {_state}");
    }

    public void Dispose()
    {
        if (_state == TransactionState.Active)
        {
            try
            {
                Rollback();
            }
            catch (Exception)
            {
                // Suppressing exception during disposal to prevent throwing from Dispose()
                // which could mask the original exception. In production, this should be logged.
            }
        }
        _lock?.Dispose();
    }
}
