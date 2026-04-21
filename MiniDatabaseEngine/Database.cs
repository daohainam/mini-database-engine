using MiniDatabaseEngine.Linq;
using MiniDatabaseEngine.Storage;
using MiniDatabaseEngine.Transaction;
using System.Collections.Concurrent;
using System.Text;

namespace MiniDatabaseEngine;

/// <summary>
/// Main database engine class
/// 
/// Thread Safety:
/// This class is designed for concurrent access. All public methods are thread-safe.
/// Uses ConcurrentDictionary for table storage and ReaderWriterLockSlim for schema operations.
/// 
/// Transaction Isolation:
/// Provides ACID transaction support with Write-Ahead Logging (WAL).
/// Isolation is achieved through locking mechanisms:
/// - Read operations: Multiple threads can read concurrently
/// - Write operations: Exclusive locks ensure atomicity
/// - Transactions: Each transaction maintains isolation via locks
/// 
/// Lock Ordering (to prevent deadlocks when nested locking occurs):
/// When multiple locks need to be acquired, always acquire them in this order:
/// 1. Database._lock (this class - schema-level operations)
/// 2. Table._lock (table-level operations)
/// 3. BPlusTree._lockObject (index operations)
/// 4. StorageEngine._lock (storage operations)
/// 5. PageCache/ExtentCache locks (cache operations)
/// </summary>
public class Database : IDisposable
{
    private const int HeaderOffsetTableCount = 12;
    private const int HeaderOffsetCatalogRootPageId = 16;
    private const int HeaderOffsetCatalogLength = 20;
    private const int HeaderOffsetVersion = 4;
    private const int HeaderOffsetNextPageId = 8;
    private const int NoPage = -1;
    private const int HeaderMagicNumber = 0x4D4445; // MDE
    private const int CatalogFormatVersion = 1;
    private const int CatalogPageMagic = 0x43415450; // CATP
    private const int CatalogPageHeaderSize = 12; // Magic(4) + NextPageId(4) + PayloadLength(4)
    private const int MaxCatalogEntrySize = 16 * 1024 * 1024; // 16 MB safety cap

    private readonly StorageEngine _storage;
    private readonly string _databaseFilePath;
    private readonly string _walFilePath;
    private readonly ConcurrentDictionary<string, Table> _tables;
    private readonly ReaderWriterLockSlim _lock;
    private readonly WALManager _walManager;
    private readonly TransactionManager _transactionManager;
    private readonly Dictionary<string, List<WALEntry>> _pendingRecoveryEntries;
    private readonly IDatabaseLogger _logger;
    private readonly bool _metricsEnabled;
    private readonly DatabaseMetrics _metrics;
    private bool _disposed;
    
    public Database(string filePath, int cacheSize = 100, bool useMemoryMappedFile = false, DatabaseOptions? options = null)
    {
        if (string.IsNullOrEmpty(filePath))
            throw new ArgumentNullException(nameof(filePath));
        
        if (!filePath.EndsWith(".mde", StringComparison.OrdinalIgnoreCase))
            filePath += ".mde";

        options ??= new DatabaseOptions();
        _databaseFilePath = filePath;
        _walFilePath = Path.ChangeExtension(filePath, ".wal");
        _logger = options.Logger ?? NullDatabaseLogger.Instance;
        _metricsEnabled = options.EnableMetrics;
        _metrics = new DatabaseMetrics();
        
        _storage = new StorageEngine(filePath, cacheSize, useMemoryMappedFile);
        _tables = new ConcurrentDictionary<string, Table>();
        _lock = new ReaderWriterLockSlim();
        _walManager = new WALManager(filePath);
        _transactionManager = new TransactionManager(_walManager, ApplyCommittedEntries);
        _pendingRecoveryEntries = new Dictionary<string, List<WALEntry>>();

        LoadCatalogFromStorage();
        RecoverFromWAL();
        Log(DatabaseLogLevel.Information, "database.opened", "Database opened", new Dictionary<string, object?>
        {
            ["path"] = _databaseFilePath
        });
    }
    
    /// <summary>
    /// Create a new table in the database
    /// </summary>
    public Table CreateTable(string tableName, List<ColumnDefinition> columns, string? primaryKeyColumn = null)
    {
        _lock.EnterWriteLock();
        try
        {
            var schema = new TableSchema(tableName, columns, primaryKeyColumn ?? "");

            if (_tables.TryGetValue(tableName, out var existingTable))
            {
                if (AreSchemasEquivalent(existingTable.Schema, schema))
                    return existingTable;

                throw new InvalidOperationException($"Table '{tableName}' already exists with a different schema");
            }

            var table = new Table(schema, _storage);
            
            _tables[tableName] = table;
            
            // Apply any pending recovery entries for this table
            if (_pendingRecoveryEntries.TryGetValue(tableName, out var entries))
            {
                foreach (var entry in entries)
                {
                    ApplyWALEntryToTable(table, entry);
                }
                _pendingRecoveryEntries.Remove(tableName);
            }

            SaveCatalogToStorage(lockSchema: false);
            TrackMetric(m => m.IncrementTablesCreated());
            Log(DatabaseLogLevel.Information, "table.created", "Table created", new Dictionary<string, object?>
            {
                ["table"] = tableName,
                ["columns"] = columns.Count
            });
            
            return table;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
    
    /// <summary>
    /// Get a table by name
    /// </summary>
    public Table GetTable(string tableName)
    {
        if (!_tables.TryGetValue(tableName, out var table))
            throw new InvalidOperationException($"Table '{tableName}' not found");
        
        return table;
    }
    
    /// <summary>
    /// Check if a table exists
    /// </summary>
    public bool TableExists(string tableName)
    {
        return _tables.ContainsKey(tableName);
    }
    
    /// <summary>
    /// Get a queryable interface for a table
    /// </summary>
    public IQueryable<DataRow> Query(string tableName)
    {
        var table = GetTable(tableName);
        return new TableQuery<DataRow>(table);
    }
    
    /// <summary>
    /// Begin a new transaction
    /// </summary>
    public Transaction.Transaction BeginTransaction()
    {
        TrackMetric(m => m.IncrementTransactionsStarted());
        Log(DatabaseLogLevel.Debug, "transaction.started", "Transaction started");
        return _transactionManager.BeginTransaction();
    }
    
    /// <summary>
    /// Insert a row into a table within a transaction
    /// </summary>
    public void Insert(string tableName, DataRow row, Transaction.Transaction? transaction = null)
    {
        var table = GetTable(tableName);
        table.Insert(row, transaction);
        TrackMetric(m => m.IncrementInserts());
        Log(DatabaseLogLevel.Debug, "row.inserted", "Row inserted", new Dictionary<string, object?>
        {
            ["table"] = tableName,
            ["transactional"] = transaction != null
        });
    }
    
    /// <summary>
    /// Update a row in a table within a transaction
    /// </summary>
    public bool Update(string tableName, object key, DataRow row, Transaction.Transaction? transaction = null)
    {
        var table = GetTable(tableName);
        var updated = table.Update(key, row, transaction);
        if (updated)
        {
            TrackMetric(m => m.IncrementUpdates());
            Log(DatabaseLogLevel.Debug, "row.updated", "Row updated", new Dictionary<string, object?>
            {
                ["table"] = tableName,
                ["transactional"] = transaction != null
            });
        }
        return updated;
    }
    
    /// <summary>
    /// Delete a row from a table within a transaction
    /// </summary>
    public bool Delete(string tableName, object key, Transaction.Transaction? transaction = null)
    {
        var table = GetTable(tableName);
        var deleted = table.Delete(key, transaction);
        if (deleted)
        {
            TrackMetric(m => m.IncrementDeletes());
            Log(DatabaseLogLevel.Debug, "row.deleted", "Row deleted", new Dictionary<string, object?>
            {
                ["table"] = tableName,
                ["transactional"] = transaction != null
            });
        }
        return deleted;
    }
    
    /// <summary>
    /// Perform a checkpoint - flush all data and mark checkpoint
    /// </summary>
    public void Checkpoint()
    {
        SaveCatalogToStorage();
        _storage.Flush();
        var activeTransactions = _transactionManager.GetActiveTransactionIds();
        var nextTransactionIdHint = _transactionManager.GetNextTransactionIdHint();
        _walManager.Checkpoint(activeTransactions, nextTransactionIdHint);
        _walManager.TruncateAfterCheckpoint();
        TrackMetric(m => m.IncrementCheckpoints());
        Log(DatabaseLogLevel.Information, "database.checkpoint", "Checkpoint completed");
    }
    
    /// <summary>
    /// Flush all changes to disk
    /// </summary>
    public void Flush()
    {
        SaveCatalogToStorage();
        _storage.Flush();
        _walManager.Flush();
        TrackMetric(m => m.IncrementFlushes());
        Log(DatabaseLogLevel.Debug, "database.flushed", "Database flushed to disk");
    }

    public DatabaseMetricsSnapshot GetMetricsSnapshot()
    {
        return _metrics.Snapshot();
    }

    public DatabaseIntegrityReport CheckIntegrity()
    {
        var issues = new List<string>();
        try
        {
            if (!File.Exists(_databaseFilePath))
            {
                issues.Add("Database file does not exist.");
            }
            else
            {
                var fileLength = new FileInfo(_databaseFilePath).Length;
                if (fileLength < Page.PageSize)
                {
                    issues.Add($"Database file is too small ({fileLength} bytes).");
                }
                else if (fileLength % Page.PageSize != 0)
                {
                    issues.Add($"Database file size {fileLength} is not aligned to page size {Page.PageSize}.");
                }

                var header = _storage.ReadPage(StorageEngine.HeaderPageId);
                using var ms = new MemoryStream(header.Data);
                using var reader = new BinaryReader(ms, Encoding.UTF8, leaveOpen: true);
                var magic = reader.ReadInt32();
                if (magic != HeaderMagicNumber)
                    issues.Add($"Invalid database header magic number: {magic}.");

                ms.Seek(HeaderOffsetVersion, SeekOrigin.Begin);
                int version = reader.ReadInt32();
                if (version <= 0)
                    issues.Add($"Invalid database version: {version}.");

                ms.Seek(HeaderOffsetNextPageId, SeekOrigin.Begin);
                int nextPageId = reader.ReadInt32();
                if (nextPageId <= 0)
                    issues.Add($"Invalid next page ID: {nextPageId}.");

                var (rootPageId, catalogLength) = ReadCatalogPointers();
                if (rootPageId < NoPage)
                    issues.Add($"Invalid catalog root page ID: {rootPageId}.");
                if (catalogLength < 0)
                    issues.Add($"Invalid catalog length: {catalogLength}.");

                if (rootPageId > 0 && catalogLength > 0)
                {
                    try
                    {
                        _ = ReadCatalogBytes(rootPageId, catalogLength);
                    }
                    catch (Exception ex) when (ex is InvalidDataException || ex is EndOfStreamException)
                    {
                        issues.Add($"Catalog validation failed: {ex.Message}");
                    }
                }
            }

            var walIntegrity = _walManager.ValidateIntegrity();
            foreach (var issue in walIntegrity.Issues)
            {
                issues.Add($"WAL: {issue}");
            }
        }
        catch (Exception ex)
        {
            issues.Add($"Integrity check failed with exception: {ex.Message}");
        }

        var report = new DatabaseIntegrityReport
        {
            IsHealthy = issues.Count == 0,
            Issues = issues
        };
        TrackMetric(m => m.IncrementIntegrityChecks());
        Log(report.IsHealthy ? DatabaseLogLevel.Information : DatabaseLogLevel.Warning, "database.integrity_check", "Integrity check completed", new Dictionary<string, object?>
        {
            ["healthy"] = report.IsHealthy,
            ["issues"] = report.Issues.Count
        });
        return report;
    }

    public string CreateBackup(string backupDirectory, string? backupName = null, bool includeWal = true)
    {
        if (string.IsNullOrWhiteSpace(backupDirectory))
            throw new ArgumentException("Backup directory is required.", nameof(backupDirectory));

        Flush();
        Directory.CreateDirectory(backupDirectory);

        var namePrefix = string.IsNullOrWhiteSpace(backupName)
            ? $"backup-{DateTimeOffset.UtcNow:yyyyMMddHHmmssfff}"
            : backupName.Trim();
        var backupPath = Path.Combine(backupDirectory, namePrefix);
        Directory.CreateDirectory(backupPath);

        var dbTargetPath = Path.Combine(backupPath, Path.GetFileName(_databaseFilePath));
        File.Copy(_databaseFilePath, dbTargetPath, overwrite: true);

        if (includeWal && File.Exists(_walFilePath))
        {
            var walTargetPath = Path.Combine(backupPath, Path.GetFileName(_walFilePath));
            File.Copy(_walFilePath, walTargetPath, overwrite: true);
        }

        TrackMetric(m => m.IncrementBackupsCreated());
        Log(DatabaseLogLevel.Information, "database.backup_created", "Backup created", new Dictionary<string, object?>
        {
            ["backupPath"] = backupPath
        });
        return backupPath;
    }

    public static void RestoreBackup(string backupPath, string destinationDatabasePath, bool overwrite = false)
    {
        if (string.IsNullOrWhiteSpace(backupPath))
            throw new ArgumentException("Backup path is required.", nameof(backupPath));
        if (string.IsNullOrWhiteSpace(destinationDatabasePath))
            throw new ArgumentException("Destination path is required.", nameof(destinationDatabasePath));
        if (!Directory.Exists(backupPath))
            throw new DirectoryNotFoundException($"Backup directory not found: {backupPath}");

        var backupDbFile = Directory.GetFiles(backupPath, "*.mde", SearchOption.TopDirectoryOnly).SingleOrDefault();
        if (backupDbFile == null)
            throw new InvalidOperationException("Backup directory does not contain a .mde database file.");

        if (!destinationDatabasePath.EndsWith(".mde", StringComparison.OrdinalIgnoreCase))
            destinationDatabasePath += ".mde";

        var destinationDirectory = Path.GetDirectoryName(destinationDatabasePath);
        if (!string.IsNullOrEmpty(destinationDirectory))
        {
            Directory.CreateDirectory(destinationDirectory);
        }

        File.Copy(backupDbFile, destinationDatabasePath, overwrite);

        var backupWalFile = Directory.GetFiles(backupPath, "*.wal", SearchOption.TopDirectoryOnly).SingleOrDefault();
        var destinationWalFile = Path.ChangeExtension(destinationDatabasePath, ".wal");
        if (backupWalFile != null)
        {
            File.Copy(backupWalFile, destinationWalFile, overwrite);
        }
        else if (overwrite && File.Exists(destinationWalFile))
        {
            File.Delete(destinationWalFile);
        }
    }
    
    private void ApplyCommittedEntries(IReadOnlyList<WALEntry> entries)
    {
        foreach (var entry in entries)
        {
            if (!_tables.TryGetValue(entry.TableName, out var table))
                continue;

            ApplyWALEntryToTable(table, entry);
        }
    }
    
    /// <summary>
    /// Apply a WAL entry to a specific table
    /// </summary>
    private void ApplyWALEntryToTable(Table table, WALEntry entry)
    {
        switch (entry.OperationType)
        {
            case WALOperationType.Insert:
                if (entry.NewValue != null && entry.Key != null)
                {
                    table.ApplyWALEntry(entry.Key, entry.NewValue, isDelete: false);
                }
                break;

            case WALOperationType.Update:
                if (entry.NewValue != null && entry.Key != null)
                {
                    table.ApplyWALEntry(entry.Key, entry.NewValue, isDelete: false);
                }
                break;

            case WALOperationType.Delete:
                if (entry.Key != null)
                {
                    table.ApplyWALEntry(entry.Key, null, isDelete: true);
                }
                break;
        }
    }
    
    /// <summary>
    /// Recover from WAL on database startup
    /// </summary>
    private void RecoverFromWAL()
    {
        int replayedEntries = 0;
        _transactionManager.RecoverFromWAL(entry =>
        {
            // If table exists, apply immediately
            if (_tables.TryGetValue(entry.TableName, out var table))
            {
                ApplyWALEntryToTable(table, entry);
                replayedEntries++;
            }
            else
            {
                // Otherwise, cache for later application when table is created
                if (!_pendingRecoveryEntries.ContainsKey(entry.TableName))
                {
                    _pendingRecoveryEntries[entry.TableName] = new List<WALEntry>();
                }
                _pendingRecoveryEntries[entry.TableName].Add(entry);
            }
        });
        Log(DatabaseLogLevel.Information, "database.recovered", "WAL recovery completed", new Dictionary<string, object?>
        {
            ["replayedEntries"] = replayedEntries
        });
    }

    private bool AreSchemasEquivalent(TableSchema left, TableSchema right)
    {
        if (!string.Equals(left.TableName, right.TableName, StringComparison.Ordinal))
            return false;

        if (!string.Equals(left.PrimaryKeyColumn, right.PrimaryKeyColumn, StringComparison.Ordinal))
            return false;

        if (left.Columns.Count != right.Columns.Count)
            return false;

        for (int i = 0; i < left.Columns.Count; i++)
        {
            var a = left.Columns[i];
            var b = right.Columns[i];
            if (!string.Equals(a.Name, b.Name, StringComparison.Ordinal) ||
                a.DataType != b.DataType ||
                a.IsNullable != b.IsNullable ||
                a.MaxLength != b.MaxLength)
            {
                return false;
            }
        }

        return true;
    }

    private void SaveCatalogToStorage(bool lockSchema = true)
    {
        if (lockSchema)
            _lock.EnterReadLock();

        try
        {
            var serialized = SerializeCatalog();
            var payloadPerPage = Page.PageSize - CatalogPageHeaderSize;

            int rootPageId = NoPage;
            if (serialized.Length > 0)
            {
                int pageCount = (serialized.Length + payloadPerPage - 1) / payloadPerPage;
                var pageIds = new List<int>(pageCount);
                for (int i = 0; i < pageCount; i++)
                {
                    pageIds.Add(_storage.AllocatePage());
                }

                int offset = 0;
                for (int i = 0; i < pageIds.Count; i++)
                {
                    int pageId = pageIds[i];
                    int nextPageId = i + 1 < pageIds.Count ? pageIds[i + 1] : NoPage;
                    int chunkLength = Math.Min(payloadPerPage, serialized.Length - offset);

                    var page = new Page(pageId);
                    using var ms = new MemoryStream(page.Data);
                    using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);
                    writer.Write(CatalogPageMagic);
                    writer.Write(nextPageId);
                    writer.Write(chunkLength);
                    writer.Write(serialized, offset, chunkLength);

                    page.IsDirty = true;
                    _storage.WritePage(page);
                    offset += chunkLength;
                }

                rootPageId = pageIds[0];
            }

            WriteCatalogPointers(rootPageId, serialized.Length, _tables.Count);
        }
        finally
        {
            if (lockSchema)
                _lock.ExitReadLock();
        }
    }

    private void LoadCatalogFromStorage()
    {
        var (rootPageId, catalogLength) = ReadCatalogPointers();
        if (rootPageId <= 0 || catalogLength <= 0)
            return;

        var data = ReadCatalogBytes(rootPageId, catalogLength);
        if (data.Length == 0)
            return;

        DeserializeCatalog(data);
    }

    private (int RootPageId, int CatalogLength) ReadCatalogPointers()
    {
        var headerPage = _storage.ReadPage(StorageEngine.HeaderPageId);
        using var ms = new MemoryStream(headerPage.Data);
        using var reader = new BinaryReader(ms, Encoding.UTF8, leaveOpen: true);

        ms.Seek(HeaderOffsetCatalogRootPageId, SeekOrigin.Begin);
        int rootPageId = reader.ReadInt32();
        int catalogLength = reader.ReadInt32();
        return (rootPageId, catalogLength);
    }

    private void WriteCatalogPointers(int rootPageId, int catalogLength, int tableCount)
    {
        var headerPage = _storage.ReadPage(StorageEngine.HeaderPageId);
        using var ms = new MemoryStream(headerPage.Data);
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        ms.Seek(HeaderOffsetTableCount, SeekOrigin.Begin);
        writer.Write(tableCount);
        writer.Write(rootPageId);
        writer.Write(catalogLength);

        headerPage.IsDirty = true;
        _storage.WritePage(headerPage);
    }

    private byte[] ReadCatalogBytes(int rootPageId, int catalogLength)
    {
        var result = new byte[catalogLength];
        int copied = 0;
        int currentPageId = rootPageId;

        while (currentPageId != NoPage && copied < catalogLength)
        {
            var page = _storage.ReadPage(currentPageId);
            using var ms = new MemoryStream(page.Data);
            using var reader = new BinaryReader(ms, Encoding.UTF8, leaveOpen: true);

            int magic = reader.ReadInt32();
            if (magic != CatalogPageMagic)
                throw new InvalidDataException($"Invalid catalog page magic at page {currentPageId}");

            int nextPageId = reader.ReadInt32();
            int payloadLength = reader.ReadInt32();
            if (payloadLength < 0 || payloadLength > Page.PageSize - CatalogPageHeaderSize)
                throw new InvalidDataException($"Invalid catalog payload length at page {currentPageId}");

            int toCopy = Math.Min(payloadLength, catalogLength - copied);
            int read = ms.Read(result, copied, toCopy);
            if (read != toCopy)
                throw new InvalidDataException($"Unexpected end of catalog payload at page {currentPageId}");

            copied += read;
            currentPageId = nextPageId;
        }

        if (copied != catalogLength)
            throw new InvalidDataException("Catalog data is incomplete");

        return result;
    }

    private byte[] SerializeCatalog()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        writer.Write(CatalogFormatVersion);

        var tables = _tables.Values
            .OrderBy(t => t.Schema.TableName, StringComparer.Ordinal)
            .ToList();

        writer.Write(tables.Count);
        foreach (var table in tables)
        {
            WriteSchema(writer, table.Schema);

            var entries = table.ExportPersistedEntries();
            writer.Write(entries.Count);
            foreach (var entry in entries)
            {
                WriteObject(writer, entry.Key);
                writer.Write(entry.Value.Length);
                writer.Write(entry.Value);
            }
        }

        writer.Flush();
        return ms.ToArray();
    }

    private void DeserializeCatalog(byte[] data)
    {
        using var ms = new MemoryStream(data);
        using var reader = new BinaryReader(ms, Encoding.UTF8, leaveOpen: true);

        int version = reader.ReadInt32();
        if (version != CatalogFormatVersion)
            throw new InvalidDataException($"Unsupported catalog format version: {version}");

        int tableCount = reader.ReadInt32();
        if (tableCount < 0)
            throw new InvalidDataException("Invalid table count in catalog");

        for (int t = 0; t < tableCount; t++)
        {
            var schema = ReadSchema(reader);
            var table = new Table(schema, _storage);

            int entryCount = reader.ReadInt32();
            if (entryCount < 0)
                throw new InvalidDataException($"Invalid entry count for table '{schema.TableName}'");

            var entries = new List<KeyValuePair<object, byte[]>>(entryCount);
            for (int i = 0; i < entryCount; i++)
            {
                var key = ReadObject(reader);
                int valueLength = reader.ReadInt32();
                if (valueLength < 0 || valueLength > MaxCatalogEntrySize)
                    throw new InvalidDataException($"Invalid row payload length for table '{schema.TableName}'");

                var value = reader.ReadBytes(valueLength);
                if (value.Length != valueLength)
                    throw new InvalidDataException($"Incomplete row payload for table '{schema.TableName}'");

                entries.Add(new KeyValuePair<object, byte[]>(key, value));
            }

            table.LoadPersistedEntries(entries);
            _tables[schema.TableName] = table;
        }
    }

    private static void WriteSchema(BinaryWriter writer, TableSchema schema)
    {
        writer.Write(schema.TableName);
        writer.Write(schema.PrimaryKeyColumn ?? string.Empty);
        writer.Write(schema.Columns.Count);

        foreach (var column in schema.Columns)
        {
            writer.Write(column.Name);
            writer.Write((int)column.DataType);
            writer.Write(column.IsNullable);
            writer.Write(column.MaxLength);
        }
    }

    private static TableSchema ReadSchema(BinaryReader reader)
    {
        var tableName = reader.ReadString();
        var primaryKeyColumn = reader.ReadString();
        int columnCount = reader.ReadInt32();
        if (columnCount <= 0)
            throw new InvalidDataException($"Table '{tableName}' has invalid column count");

        var columns = new List<ColumnDefinition>(columnCount);
        for (int i = 0; i < columnCount; i++)
        {
            var name = reader.ReadString();
            var dataType = (DataType)reader.ReadInt32();
            var isNullable = reader.ReadBoolean();
            var maxLength = reader.ReadInt32();
            columns.Add(new ColumnDefinition(name, dataType, isNullable, maxLength));
        }

        return new TableSchema(tableName, columns, primaryKeyColumn);
    }

    private static void WriteObject(BinaryWriter writer, object obj)
    {
        switch (obj)
        {
            case int i:
                writer.Write((byte)0);
                writer.Write(i);
                break;
            case long l:
                writer.Write((byte)1);
                writer.Write(l);
                break;
            case string s:
                writer.Write((byte)2);
                writer.Write(s);
                break;
            case double d:
                writer.Write((byte)3);
                writer.Write(d);
                break;
            case float f:
                writer.Write((byte)4);
                writer.Write(f);
                break;
            case bool b:
                writer.Write((byte)5);
                writer.Write(b);
                break;
            case byte bt:
                writer.Write((byte)6);
                writer.Write(bt);
                break;
            case short sh:
                writer.Write((byte)7);
                writer.Write(sh);
                break;
            case uint ui:
                writer.Write((byte)8);
                writer.Write(ui);
                break;
            case ulong ul:
                writer.Write((byte)9);
                writer.Write(ul);
                break;
            case sbyte sb:
                writer.Write((byte)10);
                writer.Write(sb);
                break;
            case ushort us:
                writer.Write((byte)11);
                writer.Write(us);
                break;
            case char c:
                writer.Write((byte)12);
                writer.Write(c);
                break;
            case decimal dec:
                writer.Write((byte)13);
                writer.Write(dec);
                break;
            case DateTime dt:
                writer.Write((byte)14);
                writer.Write(dt.ToBinary());
                break;
            default:
                throw new NotSupportedException($"Type {obj.GetType()} not supported for key serialization");
        }
    }

    private static object ReadObject(BinaryReader reader)
    {
        byte typeCode = reader.ReadByte();
        return typeCode switch
        {
            0 => reader.ReadInt32(),
            1 => reader.ReadInt64(),
            2 => reader.ReadString(),
            3 => reader.ReadDouble(),
            4 => reader.ReadSingle(),
            5 => reader.ReadBoolean(),
            6 => reader.ReadByte(),
            7 => reader.ReadInt16(),
            8 => reader.ReadUInt32(),
            9 => reader.ReadUInt64(),
            10 => reader.ReadSByte(),
            11 => reader.ReadUInt16(),
            12 => reader.ReadChar(),
            13 => reader.ReadDecimal(),
            14 => DateTime.FromBinary(reader.ReadInt64()),
            _ => throw new NotSupportedException($"Type code {typeCode} not supported")
        };
    }
    
    public void Dispose()
    {
        if (_disposed)
            return;

        SaveCatalogToStorage();
        _transactionManager?.Dispose();
        _walManager?.Dispose();
        _lock?.Dispose();
        _storage?.Dispose();
        _disposed = true;
        GC.SuppressFinalize(this);
    }

    private void TrackMetric(Action<DatabaseMetrics> updateAction)
    {
        if (_metricsEnabled)
            updateAction(_metrics);
    }

    private void Log(DatabaseLogLevel level, string eventName, string message, IReadOnlyDictionary<string, object?>? properties = null)
    {
        try
        {
            _logger.Log(new DatabaseLogEntry
            {
                Timestamp = DateTimeOffset.UtcNow,
                Level = level,
                EventName = eventName,
                Message = message,
                Properties = properties ?? new Dictionary<string, object?>()
            });
        }
        catch
        {
            // Logging must never impact database operations.
        }
    }
}
