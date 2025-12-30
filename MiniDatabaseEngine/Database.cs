using MiniDatabaseEngine.Linq;
using MiniDatabaseEngine.Storage;
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
    
    public Database(string filePath, int cacheSize = 100, bool useMemoryMappedFile = false)
    {
        if (string.IsNullOrEmpty(filePath))
            throw new ArgumentNullException(nameof(filePath));
        
        if (!filePath.EndsWith(".mde", StringComparison.OrdinalIgnoreCase))
            filePath += ".mde";
        
        _storage = new StorageEngine(filePath, cacheSize, useMemoryMappedFile);
        _tables = new ConcurrentDictionary<string, Table>();
        _lock = new ReaderWriterLockSlim();
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
    /// Insert a row into a table
    /// </summary>
    public void Insert(string tableName, DataRow row)
    {
        var table = GetTable(tableName);
        table.Insert(row);
    }
    
    /// <summary>
    /// Update a row in a table
    /// </summary>
    public bool Update(string tableName, object key, DataRow row)
    {
        var table = GetTable(tableName);
        return table.Update(key, row);
    }
    
    /// <summary>
    /// Delete a row from a table
    /// </summary>
    public bool Delete(string tableName, object key)
    {
        var table = GetTable(tableName);
        return table.Delete(key);
    }
    
    /// <summary>
    /// Flush all changes to disk
    /// </summary>
    public void Flush()
    {
        _storage.Flush();
    }
    
    public void Dispose()
    {
        _lock?.Dispose();
        _storage?.Dispose();
    }
}
