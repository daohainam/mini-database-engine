namespace MiniDatabaseEngine.Orm;

public static class DatabaseExtensions
{
    public static ObjectTable<T> CreateTable<T>(this Database database) where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(database);

        var mapper = new EntityMapper<T>();
        var table = database.CreateTable(mapper.TableName, [.. mapper.Columns], mapper.PrimaryKeyColumn);
        return new ObjectTable<T>(table, mapper);
    }

    public static ObjectTable<T> GetObjectTable<T>(this Database database) where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(database);

        var mapper = new EntityMapper<T>();
        var table = database.GetTable(mapper.TableName);
        return new ObjectTable<T>(table, mapper);
    }

    public static IQueryable<T> Query<T>(this Database database) where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(database);

        var mapper = new EntityMapper<T>();
        return database.Query(mapper.TableName)
            .AsEnumerable()
            .Select(mapper.FromDataRow)
            .AsQueryable();
    }
}
