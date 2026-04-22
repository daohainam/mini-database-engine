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
    private readonly Action<IReadOnlyList<WALEntry>> _commitApplyCallback;

    public TransactionManager(WALManager walManager, Action<IReadOnlyList<WALEntry>> commitApplyCallback)
    {
        _walManager = walManager;
        _commitApplyCallback = commitApplyCallback;
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
            var transaction = new Transaction(transactionId, _walManager, this, _commitApplyCallback);
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
    /// Gets the next transaction ID that will be assigned.
    /// </summary>
    public long GetNextTransactionIdHint()
    {
        _lock.EnterReadLock();
        try
        {
            return _nextTransactionId;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Recover from WAL - replay uncommitted transactions or rollback incomplete ones
    /// </summary>
    public void RecoverFromWAL(Action<WALEntry> applyEntry)
    {
        _lock.EnterWriteLock();
        try
        {
            var recoveryMetadata = _walManager.GetRecoveryMetadata();
            var entries = _walManager.ReadEntriesForRecovery();
            var transactions = new Dictionary<long, List<WALEntry>>();
            var committedTransactions = new HashSet<long>();

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
                    // With deferred-write transactions, data changes are not applied before commit.
                    // A rollback record therefore indicates "discard buffered writes", so there is
                    // nothing in persisted table state to undo during recovery.
                    continue;
                }
                else if (entry.OperationType != WALOperationType.BeginTransaction)
                {
                    if (!transactions.ContainsKey(entry.TransactionId))
                    {
                        transactions[entry.TransactionId] = [];
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

            // Update next transaction ID
            if (recoveryMetadata.LastSequenceNumber > 0)
            {
                var derivedNextTransactionId = entries
                    .Where(e => e.TransactionId > 0)
                    .Select(e => e.TransactionId)
                    .DefaultIfEmpty(0)
                    .Max() + 1;
                var metadataNextTransactionId = recoveryMetadata.NextTransactionIdHint;
                var computedNextTransactionId = Math.Max(derivedNextTransactionId, metadataNextTransactionId);

                _nextTransactionId = Math.Max(
                    _nextTransactionId,
                    computedNextTransactionId);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
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
            catch (Exception)
            {
                // Suppressing exception during disposal to prevent throwing from Dispose()
                // In production, this should be logged for monitoring and debugging.
            }
        }
        _activeTransactions.Clear();
        _lock?.Dispose();
    }
}
