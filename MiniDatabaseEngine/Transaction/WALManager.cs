namespace MiniDatabaseEngine.Transaction;

/// <summary>
/// Manages the Write-Ahead Log file
/// </summary>
public class WALManager : IDisposable
{
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
                byte[] data = reader.ReadBytes(length);
                var entry = WALEntry.Deserialize(data);
                entries.Add(entry);
            }
            catch (EndOfStreamException)
            {
                // Reached end of valid entries
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
            var allEntries = ReadAllEntries();
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
        _lock.EnterWriteLock();
        try
        {
            _checkpointSequence = _sequenceNumber;
            
            var entry = new WALEntry
            {
                TransactionId = -1,
                OperationType = WALOperationType.Checkpoint,
                SequenceNumber = _sequenceNumber
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

            // Keep only committed transactions after the checkpoint
            var entriesToKeep = entries
                .Where(e => e.SequenceNumber > lastCheckpoint.SequenceNumber)
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
