using MiniDatabaseEngine.Linq;
using MiniDatabaseEngine.Storage;
using MiniDatabaseEngine.Transaction;
using System.Collections.Concurrent;

namespace MiniDatabaseEngine;

/// <summary>
/// Main database engine class
/// </summary>
public class Database : IDisposable
{
    private readonly StorageEngine _storage;
    private readonly ConcurrentDictionary<string, Table> _tables;
    private readonly ReaderWriterLockSlim _lock;
    private readonly WALManager _walManager;
    private readonly TransactionManager _transactionManager;
    private readonly string _filePath;
    private readonly Dictionary<string, List<WALEntry>> _pendingRecoveryEntries;
    
    public Database(string filePath, int cacheSize = 100, bool useMemoryMappedFile = false)
    {
        if (string.IsNullOrEmpty(filePath))
            throw new ArgumentNullException(nameof(filePath));
        
        if (!filePath.EndsWith(".mde", StringComparison.OrdinalIgnoreCase))
            filePath += ".mde";
        
        _filePath = filePath;
        _storage = new StorageEngine(filePath, cacheSize, useMemoryMappedFile);
        _tables = new ConcurrentDictionary<string, Table>();
        _lock = new ReaderWriterLockSlim();
        _walManager = new WALManager(filePath);
        _transactionManager = new TransactionManager(_walManager, ApplyUndoEntry);
        _pendingRecoveryEntries = new Dictionary<string, List<WALEntry>>();
        
        // Recover from WAL if needed - entries will be cached
        RecoverFromWAL();
    }
    
    /// <summary>
    /// Create a new table in the database
    /// </summary>
    public Table CreateTable(string tableName, List<ColumnDefinition> columns, string? primaryKeyColumn = null)
    {
        _lock.EnterWriteLock();
        try
        {
            if (_tables.ContainsKey(tableName))
                throw new InvalidOperationException($"Table '{tableName}' already exists");
            
            var schema = new TableSchema(tableName, columns, primaryKeyColumn ?? "");
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
        return _transactionManager.BeginTransaction();
    }
    
    /// <summary>
    /// Insert a row into a table within a transaction
    /// </summary>
    public void Insert(string tableName, DataRow row, Transaction.Transaction? transaction = null)
    {
        var table = GetTable(tableName);
        table.Insert(row, transaction);
    }
    
    /// <summary>
    /// Update a row in a table within a transaction
    /// </summary>
    public bool Update(string tableName, object key, DataRow row, Transaction.Transaction? transaction = null)
    {
        var table = GetTable(tableName);
        return table.Update(key, row, transaction);
    }
    
    /// <summary>
    /// Delete a row from a table within a transaction
    /// </summary>
    public bool Delete(string tableName, object key, Transaction.Transaction? transaction = null)
    {
        var table = GetTable(tableName);
        return table.Delete(key, transaction);
    }
    
    /// <summary>
    /// Perform a checkpoint - flush all data and mark checkpoint
    /// Note: WAL is NOT truncated to preserve data for recovery since B+ tree is not persisted to disk
    /// </summary>
    public void Checkpoint()
    {
        _storage.Flush();
        _walManager.Checkpoint();
        // Don't truncate WAL since B+ tree data is not persisted to the main database file
        // In a real implementation, we would persist the B+ tree before truncating
        // _walManager.TruncateAfterCheckpoint();
    }
    
    /// <summary>
    /// Flush all changes to disk
    /// </summary>
    public void Flush()
    {
        _storage.Flush();
        _walManager.Flush();
    }
    
    /// <summary>
    /// Apply an undo entry during rollback
    /// </summary>
    private void ApplyUndoEntry(WALEntry entry)
    {
        if (!_tables.TryGetValue(entry.TableName, out var table))
            return;

        ApplyWALEntryToTable(table, entry);
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
        _transactionManager.RecoverFromWAL(entry =>
        {
            // If table exists, apply immediately
            if (_tables.TryGetValue(entry.TableName, out var table))
            {
                ApplyWALEntryToTable(table, entry);
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
    }
    
    public void Dispose()
    {
        _transactionManager?.Dispose();
        _walManager?.Dispose();
        _lock?.Dispose();
        _storage?.Dispose();
    }
}
