# Mini Database Engine

A simple embedded database engine in C# and .NET 10 demonstrating B+ tree data structures for efficient data storage and retrieval.

## Features

- **B+ Tree Implementation**: Efficient indexing and range queries using B+ trees
- **Multiple Data Types**: Support for byte, int, long, bool, string, float, double, and DateTime
- **File Storage**: Data persisted to `.mde` files with page-based storage
- **LINQ Support**: Query data using LINQ for intuitive data access
- **Thread-Safe**: All data modification operations are protected with reader-writer locks
- **Caching**: In-memory LRU page cache for improved performance
- **Memory-Mapped Files**: Optional support for memory-mapped files for larger datasets

## Project Structure

```
MiniDatabaseEngine/           # Main library
├── BPlusTree/               # B+ tree implementation
├── Storage/                 # Storage engine and serialization
├── Linq/                    # LINQ query provider
├── DataType.cs              # Supported data types
├── ColumnDefinition.cs      # Column schema definition
├── TableSchema.cs           # Table schema
├── DataRow.cs               # Row data structure
├── Table.cs                 # Table implementation
└── Database.cs              # Main database class

MiniDatabaseEngine.Tests/    # Unit and integration tests
MiniDatabaseEngine.Demo/     # Demo application
```

## Usage

### Creating a Database

```csharp
using MiniDatabaseEngine;

// Create or open a database file
using var db = new Database("mydata.mde", cacheSize: 100, useMemoryMappedFile: false);
```

### Creating a Table

```csharp
var columns = new List<ColumnDefinition>
{
    new ColumnDefinition("Id", DataType.Int, false),
    new ColumnDefinition("Name", DataType.String),
    new ColumnDefinition("Age", DataType.Int),
    new ColumnDefinition("Email", DataType.String)
};

var table = db.CreateTable("Users", columns, primaryKeyColumn: "Id");
```

### Inserting Data

```csharp
var row = new DataRow(table.Schema);
row["Id"] = 1;
row["Name"] = "Alice";
row["Age"] = 30;
row["Email"] = "alice@example.com";

db.Insert("Users", row);
```

### Querying Data with LINQ

```csharp
// Get all users
var allUsers = db.Query("Users").ToList();

// Query is IQueryable<DataRow>
var users = db.Query("Users")
    .Where(r => (int)r["Age"] > 25)
    .ToList();
```

### Updating Data

```csharp
var updatedRow = new DataRow(table.Schema);
updatedRow["Id"] = 1;
updatedRow["Name"] = "Alice Smith";
updatedRow["Age"] = 31;
updatedRow["Email"] = "alice.smith@example.com";

db.Update("Users", key: 1, updatedRow);
```

### Deleting Data

```csharp
db.Delete("Users", key: 1);
```

### Supported Data Types

The engine supports the following data types:

- `DataType.Byte` - 8-bit unsigned integer
- `DataType.Int` - 32-bit signed integer
- `DataType.Long` - 64-bit signed integer
- `DataType.Bool` - Boolean value
- `DataType.String` - Variable-length string
- `DataType.Float` - Single-precision floating point
- `DataType.Double` - Double-precision floating point
- `DataType.DateTime` - Date and time

## Thread Safety

All data modification operations (Insert, Update, Delete) are thread-safe and use reader-writer locks to ensure data consistency. Multiple threads can safely:

- Read data concurrently
- Insert data concurrently
- Perform mixed read/write operations

## Architecture

### B+ Tree

The B+ tree implementation provides:
- O(log n) search, insert, and delete operations
- Efficient range queries through linked leaf nodes
- Automatic node splitting when capacity is exceeded
- Support for all basic data types with custom comparers

### Storage Engine

- **Page-based storage**: Data stored in 4KB pages
- **LRU cache**: Frequently accessed pages kept in memory
- **Optional memory-mapped files**: For improved performance with larger datasets
- **Flush on demand**: Explicit control over when data is written to disk

### LINQ Provider

A custom LINQ query provider supports:
- `Where` clauses for filtering
- `OrderBy` and `OrderByDescending` for sorting
- Lazy evaluation of queries

## Running Tests

```bash
dotnet test
```

## Running the Demo

```bash
dotnet run --project MiniDatabaseEngine.Demo
```

The demo showcases:
- Creating tables with multiple data types
- Inserting, updating, and deleting data
- LINQ queries
- Concurrent access
- Data persistence

## Building

```bash
dotnet build
```

## Requirements

- .NET 10.0 or later

## License

MIT License