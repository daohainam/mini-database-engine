namespace MiniDatabaseEngine.Transaction;

public sealed class WALRecoveryMetadata
{
    public long LastSequenceNumber { get; init; }
    public long LastCheckpointSequence { get; init; }
    public IReadOnlyList<long> ActiveTransactionsAtCheckpoint { get; init; } = Array.Empty<long>();
    public long NextTransactionIdHint { get; init; }
}

/// <summary>
/// Manages the Write-Ahead Log file
/// </summary>
public class WALManager : IDisposable
{
    // Maximum size per WAL entry to prevent excessive memory allocation (1MB)
    private const int MaxWALEntrySize = 1024 * 1024;
    
    private readonly string _walFilePath;
    private readonly FileStream _walStream;
    private readonly ReaderWriterLockSlim _lock;
    private long _sequenceNumber;
    private long _checkpointSequence;

    public WALManager(string databaseFilePath)
    {
        _walFilePath = Path.ChangeExtension(databaseFilePath, ".wal");
        _lock = new ReaderWriterLockSlim();
        _sequenceNumber = 0;
        _checkpointSequence = 0;

        // Open or create WAL file
        _walStream = new FileStream(
            _walFilePath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.Read);

        if (_walStream.Length > 0)
        {
            LoadLastSequenceNumber();
        }
    }

    /// <summary>
    /// Append a WAL entry to the log
    /// </summary>
    public void AppendEntry(WALEntry entry)
    {
        _lock.EnterWriteLock();
        try
        {
            entry.SequenceNumber = ++_sequenceNumber;
            var serialized = entry.Serialize();

            // Write entry length followed by entry data
            _walStream.Seek(0, SeekOrigin.End);
            using var writer = new BinaryWriter(_walStream, System.Text.Encoding.UTF8, leaveOpen: true);
            writer.Write(serialized.Length);
            writer.Write(serialized);
            writer.Flush();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Read all entries from the WAL
    /// </summary>
    public List<WALEntry> ReadAllEntries()
    {
        _lock.EnterReadLock();
        try
        {
            return ReadAllEntriesInternal();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Internal method to read entries without locking (for use when lock is already held)
    /// </summary>
    private List<WALEntry> ReadAllEntriesInternal()
    {
        var entries = new List<WALEntry>();
        _walStream.Seek(0, SeekOrigin.Begin);

        using var reader = new BinaryReader(_walStream, System.Text.Encoding.UTF8, leaveOpen: true);
        while (_walStream.Position < _walStream.Length)
        {
            try
            {
                int length = reader.ReadInt32();
                
                // Validate length to prevent excessive memory allocation
                if (length <= 0 || length > MaxWALEntrySize)
                {
                    // Corrupted WAL file - stop reading
                    break;
                }
                
                byte[] data = reader.ReadBytes(length);
                if (data.Length != length)
                {
                    // Incomplete entry - stop reading
                    break;
                }
                
                var entry = WALEntry.Deserialize(data);
                entries.Add(entry);
            }
            catch (EndOfStreamException)
            {
                // Reached end of valid entries
                break;
            }
            catch (InvalidDataException)
            {
                // Corrupted entry payload - stop at last valid entry
                break;
            }
        }

        return entries;
    }

    /// <summary>
    /// Read entries after a specific sequence number
    /// </summary>
    public List<WALEntry> ReadEntriesAfter(long sequenceNumber)
    {
        _lock.EnterReadLock();
        try
        {
            var allEntries = ReadAllEntriesInternal();
            return allEntries.Where(e => e.SequenceNumber > sequenceNumber).ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Perform a checkpoint - mark that all entries up to this point have been applied
    /// </summary>
    public void Checkpoint()
    {
        Checkpoint(Array.Empty<long>(), 0);
    }

    /// <summary>
    /// Perform a checkpoint and persist recovery metadata.
    /// </summary>
    public void Checkpoint(IReadOnlyList<long> activeTransactionIds, long nextTransactionIdHint)
    {
        _lock.EnterWriteLock();
        try
        {
            var checkpointSequence = ++_sequenceNumber;
            _checkpointSequence = checkpointSequence;
            
            var entry = new WALEntry
            {
                TransactionId = -1,
                OperationType = WALOperationType.Checkpoint,
                SequenceNumber = checkpointSequence,
                CheckpointActiveTransactionIds = activeTransactionIds
                    .Distinct()
                    .OrderBy(x => x)
                    .ToList(),
                CheckpointNextTransactionId = nextTransactionIdHint
            };

            var serialized = entry.Serialize();
            _walStream.Seek(0, SeekOrigin.End);
            using var writer = new BinaryWriter(_walStream, System.Text.Encoding.UTF8, leaveOpen: true);
            writer.Write(serialized.Length);
            writer.Write(serialized);
            writer.Flush();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Truncate the WAL file after a checkpoint
    /// </summary>
    public void TruncateAfterCheckpoint()
    {
        _lock.EnterWriteLock();
        try
        {
            var entries = ReadAllEntriesInternal();
            
            // Find the last checkpoint
            var lastCheckpoint = entries.LastOrDefault(e => e.OperationType == WALOperationType.Checkpoint);
            if (lastCheckpoint == null)
                return;
            
            var activeAtCheckpoint = new HashSet<long>(lastCheckpoint.CheckpointActiveTransactionIds);

            // Keep entries needed for safe recovery:
            // 1) latest checkpoint metadata entry
            // 2) all entries after checkpoint
            // 3) all entries for transactions that were active at checkpoint
            var entriesToKeep = entries
                .Where(e =>
                    e.SequenceNumber >= lastCheckpoint.SequenceNumber ||
                    (e.TransactionId > 0 && activeAtCheckpoint.Contains(e.TransactionId)))
                .OrderBy(e => e.SequenceNumber)
                .ToList();

            // Rewrite the WAL file
            _walStream.SetLength(0);
            _walStream.Seek(0, SeekOrigin.Begin);

            using var writer = new BinaryWriter(_walStream, System.Text.Encoding.UTF8, leaveOpen: true);
            foreach (var entry in entriesToKeep)
            {
                var serialized = entry.Serialize();
                writer.Write(serialized.Length);
                writer.Write(serialized);
            }

            writer.Flush();
            _walStream.Flush(true);

            if (entriesToKeep.Count > 0)
            {
                _sequenceNumber = entriesToKeep.Max(e => e.SequenceNumber);
                var latestCheckpoint = entriesToKeep.LastOrDefault(e => e.OperationType == WALOperationType.Checkpoint);
                _checkpointSequence = latestCheckpoint?.SequenceNumber ?? 0;
            }
            else
            {
                _sequenceNumber = 0;
                _checkpointSequence = 0;
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Clear the entire WAL file
    /// </summary>
    public void Clear()
    {
        _lock.EnterWriteLock();
        try
        {
            _walStream.SetLength(0);
            _walStream.Flush(true);
            _sequenceNumber = 0;
            _checkpointSequence = 0;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Force flush all pending writes to disk
    /// </summary>
    public void Flush()
    {
        _lock.EnterWriteLock();
        try
        {
            _walStream.Flush(true);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
    
    /// <summary>
    /// Returns metadata describing the latest recovery boundary.
    /// </summary>
    public WALRecoveryMetadata GetRecoveryMetadata()
    {
        _lock.EnterReadLock();
        try
        {
            var entries = ReadAllEntriesInternal();
            var lastCheckpoint = entries.LastOrDefault(e => e.OperationType == WALOperationType.Checkpoint);

            return new WALRecoveryMetadata
            {
                LastSequenceNumber = entries.Count == 0 ? 0 : entries.Max(e => e.SequenceNumber),
                LastCheckpointSequence = lastCheckpoint?.SequenceNumber ?? 0,
                ActiveTransactionsAtCheckpoint = lastCheckpoint?.CheckpointActiveTransactionIds.ToArray() ?? Array.Empty<long>(),
                NextTransactionIdHint = lastCheckpoint?.CheckpointNextTransactionId ?? 0
            };
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Read the minimal set of entries needed for crash recovery.
    /// </summary>
    public List<WALEntry> ReadEntriesForRecovery()
    {
        _lock.EnterReadLock();
        try
        {
            var entries = ReadAllEntriesInternal();
            var lastCheckpoint = entries.LastOrDefault(e => e.OperationType == WALOperationType.Checkpoint);
            if (lastCheckpoint == null)
                return entries;

            var activeAtCheckpoint = new HashSet<long>(lastCheckpoint.CheckpointActiveTransactionIds);
            return entries
                .Where(e =>
                    e.SequenceNumber >= lastCheckpoint.SequenceNumber ||
                    (e.TransactionId > 0 && activeAtCheckpoint.Contains(e.TransactionId)))
                .OrderBy(e => e.SequenceNumber)
                .ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private void LoadLastSequenceNumber()
    {
        var entries = ReadAllEntries();
        if (entries.Count > 0)
        {
            _sequenceNumber = entries.Max(e => e.SequenceNumber);
        }

        var lastCheckpoint = entries.LastOrDefault(e => e.OperationType == WALOperationType.Checkpoint);
        if (lastCheckpoint != null)
        {
            _checkpointSequence = lastCheckpoint.SequenceNumber;
        }
    }

    public void Dispose()
    {
        Flush();
        _walStream?.Dispose();
        _lock?.Dispose();
    }
}
