using MiniDatabaseEngine.BPlusTree;
using MiniDatabaseEngine.Storage;
using MiniDatabaseEngine.Transaction;
using System.Collections;

namespace MiniDatabaseEngine;

/// <summary>
/// Represents a table in the database with B+ Tree indexing
/// </summary>
public class Table
{
    private readonly TableSchema _schema;
    private readonly BPlusTree.BPlusTree _index;
    private readonly StorageEngine _storage;
    private readonly ReaderWriterLockSlim _lock;
    private int _nextRowId;
    
    public Table(TableSchema schema, StorageEngine storage)
    {
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _lock = new ReaderWriterLockSlim();
        _nextRowId = 0;
        
        // Create B+ Tree index on primary key
        var pkColumn = _schema.Columns.FirstOrDefault(c => c.Name == _schema.PrimaryKeyColumn);
        var keyType = pkColumn?.DataType ?? DataType.Int;
        _index = new BPlusTree.BPlusTree(4, keyType);
    }
    
    public TableSchema Schema => _schema;
    
    /// <summary>
    /// Insert a new row into the table
    /// </summary>
    public void Insert(DataRow row, Transaction.Transaction? transaction = null)
    {
        _lock.EnterWriteLock();
        try
        {
            var key = GetPrimaryKey(row);
            
            // Serialize row data
            var serialized = SerializeRow(row);
            
            // Log to transaction if active
            if (transaction != null)
            {
                transaction.LogInsert(_schema.TableName, key, serialized);
            }
            
            // Store in B+ Tree
            _index.Insert(key, serialized);
            _nextRowId++;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
    
    /// <summary>
    /// Update a row by primary key
    /// </summary>
    public bool Update(object key, DataRow row, Transaction.Transaction? transaction = null)
    {
        _lock.EnterWriteLock();
        try
        {
            var existing = _index.Search(key);
            if (existing == null)
                return false;
            
            var oldValue = (byte[])existing;
            var serialized = SerializeRow(row);
            
            // Log to transaction if active
            if (transaction != null)
            {
                transaction.LogUpdate(_schema.TableName, key, oldValue, serialized);
            }
            
            _index.Insert(key, serialized);
            return true;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
    
    /// <summary>
    /// Delete a row by primary key
    /// </summary>
    public bool Delete(object key, Transaction.Transaction? transaction = null)
    {
        _lock.EnterWriteLock();
        try
        {
            var existing = _index.Search(key);
            if (existing == null)
                return false;
            
            var oldValue = (byte[])existing;
            
            // Log to transaction if active
            if (transaction != null)
            {
                transaction.LogDelete(_schema.TableName, key, oldValue);
            }
            
            return _index.Delete(key);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
    
    /// <summary>
    /// Apply a WAL entry to the table (used during recovery)
    /// </summary>
    internal void ApplyWALEntry(object key, byte[]? value, bool isDelete)
    {
        _lock.EnterWriteLock();
        try
        {
            if (isDelete)
            {
                _index.Delete(key);
            }
            else if (value != null)
            {
                _index.Insert(key, value);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
    
    /// <summary>
    /// Select a row by primary key
    /// </summary>
    public DataRow? SelectByKey(object key)
    {
        _lock.EnterReadLock();
        try
        {
            var serialized = _index.Search(key);
            if (serialized == null)
                return null;
            
            return DeserializeRow((byte[])serialized);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }
    
    /// <summary>
    /// Select all rows
    /// </summary>
    public IEnumerable<DataRow> SelectAll()
    {
        _lock.EnterReadLock();
        try
        {
            foreach (var kvp in _index.GetAll())
            {
                yield return DeserializeRow((byte[])kvp.Value!);
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }
    
    private object GetPrimaryKey(DataRow row)
    {
        if (string.IsNullOrEmpty(_schema.PrimaryKeyColumn))
        {
            return _nextRowId;
        }
        
        var key = row[_schema.PrimaryKeyColumn];
        if (key == null)
            throw new InvalidOperationException("Primary key cannot be null");
        
        return key;
    }
    
    private byte[] SerializeRow(DataRow row)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        
        var values = row.GetValues();
        for (int i = 0; i < _schema.Columns.Count; i++)
        {
            var serialized = DataSerializer.Serialize(values[i], _schema.Columns[i].DataType);
            writer.Write(serialized.Length);
            writer.Write(serialized);
        }
        
        return ms.ToArray();
    }
    
    private DataRow DeserializeRow(byte[] data)
    {
        using var ms = new MemoryStream(data);
        using var reader = new BinaryReader(ms);
        
        var values = new object?[_schema.Columns.Count];
        for (int i = 0; i < _schema.Columns.Count; i++)
        {
            int length = reader.ReadInt32();
            byte[] columnData = reader.ReadBytes(length);
            values[i] = DataSerializer.Deserialize(columnData, _schema.Columns[i].DataType);
        }
        
        return new DataRow(_schema, values);
    }
}
