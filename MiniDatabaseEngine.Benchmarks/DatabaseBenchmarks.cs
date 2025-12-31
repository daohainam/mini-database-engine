using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using MiniDatabaseEngine;

namespace MiniDatabaseEngine.Benchmarks;

[MemoryDiagnoser]
public class DatabaseBenchmarks
{
    private Database? _db;
    private string? _dbPath;
    private Table? _smallRecordTable;
    private Table? _largeRecordTable;
    private const int RecordCount = 1000;

    [GlobalSetup]
    public void Setup()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"benchmark_{Guid.NewGuid()}.mde");
        _db = new Database(_dbPath, cacheSize: 500, useMemoryMappedFile: false);

        // Create table for small records (3 columns)
        var smallColumns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String),
            new("Value", DataType.Int)
        };
        _smallRecordTable = _db.CreateTable("SmallRecords", smallColumns, "Id");

        // Create table for large records (15 columns)
        var largeColumns = new List<ColumnDefinition>
        {
            new("Id", DataType.Int, false),
            new("Name", DataType.String),
            new("Email", DataType.String),
            new("Age", DataType.Int),
            new("IsActive", DataType.Bool),
            new("Balance", DataType.Double),
            new("CreatedAt", DataType.DateTime),
            new("UpdatedAt", DataType.DateTime),
            new("Description", DataType.String),
            new("Address", DataType.String),
            new("City", DataType.String),
            new("Country", DataType.String),
            new("PostalCode", DataType.String),
            new("Phone", DataType.String),
            new("Score", DataType.Double)
        };
        _largeRecordTable = _db.CreateTable("LargeRecords", largeColumns, "Id");

        // Pre-populate tables for read benchmarks
        var baseDate = new DateTime(2024, 1, 1);
        for (int i = 1; i <= RecordCount; i++)
        {
            var smallRow = new DataRow(_smallRecordTable.Schema);
            smallRow["Id"] = i;
            smallRow["Name"] = $"Record {i}";
            smallRow["Value"] = i * 10;
            _db.Insert("SmallRecords", smallRow);

            var largeRow = new DataRow(_largeRecordTable.Schema);
            largeRow["Id"] = i;
            largeRow["Name"] = $"User {i}";
            largeRow["Email"] = $"user{i}@example.com";
            largeRow["Age"] = 20 + (i % 50);
            largeRow["IsActive"] = i % 2 == 0;
            largeRow["Balance"] = 1000.0 * i;
            largeRow["CreatedAt"] = baseDate.AddDays(-i);
            largeRow["UpdatedAt"] = baseDate;
            largeRow["Description"] = $"This is a longer description for user {i} with some additional text to make the record larger.";
            largeRow["Address"] = $"{i} Main Street";
            largeRow["City"] = "Sample City";
            largeRow["Country"] = "Sample Country";
            largeRow["PostalCode"] = $"{10000 + i}";
            largeRow["Phone"] = $"+1-555-{1000 + i}";
            largeRow["Score"] = 50.5 + (i % 100);
            _db.Insert("LargeRecords", largeRow);
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _db?.Dispose();
        if (_dbPath != null && File.Exists(_dbPath))
            File.Delete(_dbPath);
    }

    [Benchmark]
    public DataRow? ReadSmallRecords()
    {
        if (_smallRecordTable == null) return null;

        DataRow? lastRecord = null;
        for (int i = 1; i <= 100; i++)
        {
            lastRecord = _smallRecordTable.SelectByKey(i);
        }
        return lastRecord;
    }

    [Benchmark]
    public DataRow? ReadLargeRecords()
    {
        if (_largeRecordTable == null) return null;

        DataRow? lastRecord = null;
        for (int i = 1; i <= 100; i++)
        {
            lastRecord = _largeRecordTable.SelectByKey(i);
        }
        return lastRecord;
    }

    [Benchmark]
    public void InsertSmallRecords()
    {
        if (_db == null || _smallRecordTable == null) return;

        for (int i = RecordCount + 1; i <= RecordCount + 100; i++)
        {
            var row = new DataRow(_smallRecordTable.Schema);
            row["Id"] = i;
            row["Name"] = $"Record {i}";
            row["Value"] = i * 10;
            _db.Insert("SmallRecords", row);
        }

        // Cleanup inserted records
        for (int i = RecordCount + 1; i <= RecordCount + 100; i++)
        {
            _db.Delete("SmallRecords", i);
        }
    }

    [Benchmark]
    public void InsertLargeRecords()
    {
        if (_db == null || _largeRecordTable == null) return;

        var baseDate = new DateTime(2024, 1, 1);
        for (int i = RecordCount + 1; i <= RecordCount + 100; i++)
        {
            var row = new DataRow(_largeRecordTable.Schema);
            row["Id"] = i;
            row["Name"] = $"User {i}";
            row["Email"] = $"user{i}@example.com";
            row["Age"] = 20 + (i % 50);
            row["IsActive"] = i % 2 == 0;
            row["Balance"] = 1000.0 * i;
            row["CreatedAt"] = baseDate.AddDays(-i);
            row["UpdatedAt"] = baseDate;
            row["Description"] = $"This is a longer description for user {i} with some additional text to make the record larger.";
            row["Address"] = $"{i} Main Street";
            row["City"] = "Sample City";
            row["Country"] = "Sample Country";
            row["PostalCode"] = $"{10000 + i}";
            row["Phone"] = $"+1-555-{1000 + i}";
            row["Score"] = 50.5 + (i % 100);
            _db.Insert("LargeRecords", row);
        }

        // Cleanup inserted records
        for (int i = RecordCount + 1; i <= RecordCount + 100; i++)
        {
            _db.Delete("LargeRecords", i);
        }
    }

    [Benchmark]
    public void UpdateSmallRecords()
    {
        if (_db == null || _smallRecordTable == null) return;

        for (int i = 1; i <= 100; i++)
        {
            var row = new DataRow(_smallRecordTable.Schema);
            row["Id"] = i;
            row["Name"] = $"Updated Record {i}";
            row["Value"] = i * 20;
            _db.Update("SmallRecords", i, row);
        }

        // Restore original values
        for (int i = 1; i <= 100; i++)
        {
            var row = new DataRow(_smallRecordTable.Schema);
            row["Id"] = i;
            row["Name"] = $"Record {i}";
            row["Value"] = i * 10;
            _db.Update("SmallRecords", i, row);
        }
    }

    [Benchmark]
    public void UpdateLargeRecords()
    {
        if (_db == null || _largeRecordTable == null) return;

        var baseDate = new DateTime(2024, 1, 1);
        for (int i = 1; i <= 100; i++)
        {
            var row = new DataRow(_largeRecordTable.Schema);
            row["Id"] = i;
            row["Name"] = $"Updated User {i}";
            row["Email"] = $"updated{i}@example.com";
            row["Age"] = 30 + (i % 50);
            row["IsActive"] = i % 3 == 0;
            row["Balance"] = 2000.0 * i;
            row["CreatedAt"] = baseDate.AddDays(-i);
            row["UpdatedAt"] = baseDate;
            row["Description"] = $"Updated description for user {i}";
            row["Address"] = $"{i} Updated Street";
            row["City"] = "Updated City";
            row["Country"] = "Updated Country";
            row["PostalCode"] = $"{20000 + i}";
            row["Phone"] = $"+1-555-{2000 + i}";
            row["Score"] = 75.5 + (i % 100);
            _db.Update("LargeRecords", i, row);
        }

        // Restore original values
        for (int i = 1; i <= 100; i++)
        {
            var row = new DataRow(_largeRecordTable.Schema);
            row["Id"] = i;
            row["Name"] = $"User {i}";
            row["Email"] = $"user{i}@example.com";
            row["Age"] = 20 + (i % 50);
            row["IsActive"] = i % 2 == 0;
            row["Balance"] = 1000.0 * i;
            row["CreatedAt"] = baseDate.AddDays(-i);
            row["UpdatedAt"] = baseDate;
            row["Description"] = $"This is a longer description for user {i} with some additional text to make the record larger.";
            row["Address"] = $"{i} Main Street";
            row["City"] = "Sample City";
            row["Country"] = "Sample Country";
            row["PostalCode"] = $"{10000 + i}";
            row["Phone"] = $"+1-555-{1000 + i}";
            row["Score"] = 50.5 + (i % 100);
            _db.Update("LargeRecords", i, row);
        }
    }

    [Benchmark]
    public void DeleteAndReInsertSmallRecords()
    {
        if (_db == null || _smallRecordTable == null) return;

        // Delete records
        for (int i = 1; i <= 100; i++)
        {
            _db.Delete("SmallRecords", i);
        }

        // Re-insert records
        for (int i = 1; i <= 100; i++)
        {
            var row = new DataRow(_smallRecordTable.Schema);
            row["Id"] = i;
            row["Name"] = $"Record {i}";
            row["Value"] = i * 10;
            _db.Insert("SmallRecords", row);
        }
    }

    [Benchmark]
    public void DeleteAndReInsertLargeRecords()
    {
        if (_db == null || _largeRecordTable == null) return;

        // Delete records
        for (int i = 1; i <= 100; i++)
        {
            _db.Delete("LargeRecords", i);
        }

        // Re-insert records
        var baseDate = new DateTime(2024, 1, 1);
        for (int i = 1; i <= 100; i++)
        {
            var row = new DataRow(_largeRecordTable.Schema);
            row["Id"] = i;
            row["Name"] = $"User {i}";
            row["Email"] = $"user{i}@example.com";
            row["Age"] = 20 + (i % 50);
            row["IsActive"] = i % 2 == 0;
            row["Balance"] = 1000.0 * i;
            row["CreatedAt"] = baseDate.AddDays(-i);
            row["UpdatedAt"] = baseDate;
            row["Description"] = $"This is a longer description for user {i} with some additional text to make the record larger.";
            row["Address"] = $"{i} Main Street";
            row["City"] = "Sample City";
            row["Country"] = "Sample Country";
            row["PostalCode"] = $"{10000 + i}";
            row["Phone"] = $"+1-555-{1000 + i}";
            row["Score"] = 50.5 + (i % 100);
            _db.Insert("LargeRecords", row);
        }
    }
}
