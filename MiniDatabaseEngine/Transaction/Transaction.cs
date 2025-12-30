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
    private readonly Action<WALEntry> _undoCallback;

    internal Transaction(long transactionId, WALManager walManager, TransactionManager transactionManager, Action<WALEntry> undoCallback)
    {
        _transactionId = transactionId;
        _walManager = walManager;
        _transactionManager = transactionManager;
        _undoCallback = undoCallback;
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

            // Undo operations in reverse order
            for (int i = _entries.Count - 1; i >= 0; i--)
            {
                var entry = _entries[i];
                var undoEntry = CreateUndoEntry(entry);
                _undoCallback(undoEntry);
            }

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

    private WALEntry CreateUndoEntry(WALEntry entry)
    {
        var undoEntry = new WALEntry
        {
            TransactionId = entry.TransactionId,
            TableName = entry.TableName,
            Key = entry.Key,
            SequenceNumber = entry.SequenceNumber
        };

        switch (entry.OperationType)
        {
            case WALOperationType.Insert:
                // Undo insert by deleting
                undoEntry.OperationType = WALOperationType.Delete;
                undoEntry.OldValue = entry.NewValue;
                break;

            case WALOperationType.Update:
                // Undo update by restoring old value
                undoEntry.OperationType = WALOperationType.Update;
                undoEntry.NewValue = entry.OldValue;
                undoEntry.OldValue = entry.NewValue;
                break;

            case WALOperationType.Delete:
                // Undo delete by inserting old value
                undoEntry.OperationType = WALOperationType.Insert;
                undoEntry.NewValue = entry.OldValue;
                break;
        }

        return undoEntry;
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
            catch
            {
                // Ignore errors during disposal
            }
        }
        _lock?.Dispose();
    }
}
