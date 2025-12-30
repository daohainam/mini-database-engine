using System.Collections.Concurrent;

namespace MiniDatabaseEngine.Transaction;

/// <summary>
/// Manages active transactions and provides isolation
/// </summary>
public class TransactionManager : IDisposable
{
    private readonly WALManager _walManager;
    private readonly ConcurrentDictionary<long, Transaction> _activeTransactions;
    private long _nextTransactionId;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Action<WALEntry> _undoCallback;

    public TransactionManager(WALManager walManager, Action<WALEntry> undoCallback)
    {
        _walManager = walManager;
        _undoCallback = undoCallback;
        _activeTransactions = new ConcurrentDictionary<long, Transaction>();
        _nextTransactionId = 1;
        _lock = new ReaderWriterLockSlim();
    }

    /// <summary>
    /// Begin a new transaction
    /// </summary>
    public Transaction BeginTransaction()
    {
        _lock.EnterWriteLock();
        try
        {
            long transactionId = _nextTransactionId++;
            var transaction = new Transaction(transactionId, _walManager, this, _undoCallback);
            _activeTransactions[transactionId] = transaction;
            return transaction;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Get an active transaction by ID
    /// </summary>
    public Transaction? GetTransaction(long transactionId)
    {
        _activeTransactions.TryGetValue(transactionId, out var transaction);
        return transaction;
    }

    /// <summary>
    /// Check if a transaction is active
    /// </summary>
    public bool IsTransactionActive(long transactionId)
    {
        return _activeTransactions.ContainsKey(transactionId);
    }

    /// <summary>
    /// Complete a transaction (commit or rollback)
    /// </summary>
    internal void CompleteTransaction(long transactionId)
    {
        _activeTransactions.TryRemove(transactionId, out _);
    }

    /// <summary>
    /// Get all active transaction IDs
    /// </summary>
    public IReadOnlyList<long> GetActiveTransactionIds()
    {
        return _activeTransactions.Keys.ToList();
    }

    /// <summary>
    /// Recover from WAL - replay uncommitted transactions or rollback incomplete ones
    /// </summary>
    public void RecoverFromWAL(Action<WALEntry> applyEntry)
    {
        _lock.EnterWriteLock();
        try
        {
            var entries = _walManager.ReadAllEntries();
            var transactions = new Dictionary<long, List<WALEntry>>();
            var committedTransactions = new HashSet<long>();
            var rolledBackTransactions = new HashSet<long>();

            // Group entries by transaction
            foreach (var entry in entries)
            {
                if (entry.OperationType == WALOperationType.Checkpoint)
                    continue;

                if (entry.OperationType == WALOperationType.Commit)
                {
                    committedTransactions.Add(entry.TransactionId);
                }
                else if (entry.OperationType == WALOperationType.Rollback)
                {
                    rolledBackTransactions.Add(entry.TransactionId);
                }
                else if (entry.OperationType != WALOperationType.BeginTransaction)
                {
                    if (!transactions.ContainsKey(entry.TransactionId))
                    {
                        transactions[entry.TransactionId] = new List<WALEntry>();
                    }
                    transactions[entry.TransactionId].Add(entry);
                }
            }

            // Apply committed transactions
            foreach (var txnId in committedTransactions)
            {
                if (transactions.TryGetValue(txnId, out var txnEntries))
                {
                    foreach (var entry in txnEntries)
                    {
                        applyEntry(entry);
                    }
                }
            }

            // Rollback uncommitted transactions (not committed and not rolled back)
            var uncommittedTransactions = transactions.Keys
                .Where(id => !committedTransactions.Contains(id) && !rolledBackTransactions.Contains(id))
                .ToList();

            foreach (var txnId in uncommittedTransactions)
            {
                if (transactions.TryGetValue(txnId, out var txnEntries))
                {
                    // Undo operations in reverse order
                    foreach (var entry in txnEntries.AsEnumerable().Reverse())
                    {
                        RollbackEntry(entry, applyEntry);
                    }
                }
            }

            // Update next transaction ID
            if (entries.Count > 0)
            {
                _nextTransactionId = entries.Max(e => e.TransactionId) + 1;
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private void RollbackEntry(WALEntry entry, Action<WALEntry> applyEntry)
    {
        // Create an undo entry
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

        applyEntry(undoEntry);
    }

    public void Dispose()
    {
        // Complete all active transactions
        foreach (var transaction in _activeTransactions.Values)
        {
            try
            {
                transaction.Rollback();
            }
            catch
            {
                // Ignore errors
            }
        }
        _activeTransactions.Clear();
        _lock?.Dispose();
    }
}
