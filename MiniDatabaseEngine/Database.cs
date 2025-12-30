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
        _transactionManager = new TransactionManager(_walManager);
        
        // Recover from WAL if needed
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
    /// Perform a checkpoint - flush all data and truncate WAL
    /// </summary>
    public void Checkpoint()
    {
        _storage.Flush();
        _walManager.Checkpoint();
        _walManager.TruncateAfterCheckpoint();
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
    /// Recover from WAL on database startup
    /// </summary>
    private void RecoverFromWAL()
    {
        _transactionManager.RecoverFromWAL(entry =>
        {
            if (!_tables.TryGetValue(entry.TableName, out var table))
                return;

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
