namespace MiniDatabaseEngine.Orm;

public sealed class ObjectTable<T>(Table table, EntityMapper<T> mapper) where T : class, new()
{
    private readonly Table _table = table ?? throw new ArgumentNullException(nameof(table));
    private readonly EntityMapper<T> _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));

    public TableSchema Schema => _table.Schema;
    public string TableName => _table.Schema.TableName;

    public void Insert(T entity, Transaction.Transaction? transaction = null)
    {
        var row = _mapper.ToDataRow(_table.Schema, entity);
        _table.Insert(row, transaction);
    }

    public bool Update(T entity, Transaction.Transaction? transaction = null)
    {
        var key = _mapper.GetPrimaryKeyValue(entity);
        var row = _mapper.ToDataRow(_table.Schema, entity);
        return _table.Update(key, row, transaction);
    }

    public bool Delete(object key, Transaction.Transaction? transaction = null)
    {
        return _table.Delete(key, transaction);
    }

    public T? SelectByKey(object key)
    {
        var row = _table.SelectByKey(key);
        return row == null ? null : _mapper.FromDataRow(row);
    }

    public IEnumerable<T> SelectAll()
    {
        return _table.SelectAll().Select(_mapper.FromDataRow);
    }
}
