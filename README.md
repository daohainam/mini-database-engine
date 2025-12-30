# Mini Database Engine

A simple embedded database engine in C# and .NET 10 demonstrating B+ tree data structures for efficient data storage and retrieval with ACID transaction support.

## Features

- **B+ Tree Implementation**: Efficient indexing and range queries using B+ trees
- **ACID Transactions**: Full ACID transaction support with Write-Ahead Logging (WAL)
- **Crash Recovery**: Automatic recovery from WAL on database restart
- **Multiple Data Types**: Support for byte, sbyte, short, ushort, int, uint, long, ulong, bool, char, string, float, double, decimal, and DateTime
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
├── Transaction/             # Transaction management and WAL
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

### Using Transactions

#### Basic Transaction with Commit

```csharp
// Begin a transaction
using var txn = db.BeginTransaction();

// Perform multiple operations within the transaction
var row1 = new DataRow(table.Schema);
row1["Id"] = 1;
row1["Name"] = "Alice";
db.Insert("Users", row1, txn);

var row2 = new DataRow(table.Schema);
row2["Id"] = 2;
row2["Name"] = "Bob";
db.Insert("Users", row2, txn);

// Commit the transaction to make changes permanent
txn.Commit();
```

#### Transaction with Rollback

```csharp
using var txn = db.BeginTransaction();

// Perform operations
db.Insert("Users", row, txn);
db.Update("Users", key: 1, updatedRow, txn);

// Rollback to undo all changes
txn.Rollback();
```

#### Automatic Rollback

```csharp
// Transaction automatically rolls back if not committed
using (var txn = db.BeginTransaction())
{
    db.Insert("Users", row, txn);
    // If an exception occurs here, transaction is rolled back
    throw new Exception("Something went wrong");
} // Transaction is automatically rolled back on disposal
```

#### Bank Transfer Example (Atomicity)

```csharp
// Transfer money between accounts - either both operations succeed or both fail
using var txn = db.BeginTransaction();

try
{
    // Debit from account 1
    var account1 = accountsTable.SelectByKey(1);
    account1["Balance"] = (double)account1["Balance"] - 100.0;
    db.Update("Accounts", 1, account1, txn);
    
    // Credit to account 2
    var account2 = accountsTable.SelectByKey(2);
    account2["Balance"] = (double)account2["Balance"] + 100.0;
    db.Update("Accounts", 2, account2, txn);
    
    // Commit both changes atomically
    txn.Commit();
}
catch
{
    txn.Rollback();
    throw;
}
```

#### Checkpoint

```csharp
// Flush all data and create a checkpoint in the WAL
db.Checkpoint();
```

### Crash Recovery

The database automatically recovers from crashes by replaying committed transactions from the Write-Ahead Log (WAL):

```csharp
// After a crash, simply reopen the database
using var db = new Database("mydata.mde");

// Recreate tables with same schema
var table = db.CreateTable("Users", columns, "Id");

// Data from committed transactions is automatically recovered
var user = table.SelectByKey(1); // Returns data from WAL
```

### Supported Data Types

The engine supports the following data types:

- `DataType.Byte` - 8-bit unsigned integer (0 to 255)
- `DataType.SByte` - 8-bit signed integer (-128 to 127)
- `DataType.Short` - 16-bit signed integer (-32,768 to 32,767)
- `DataType.UShort` - 16-bit unsigned integer (0 to 65,535)
- `DataType.Int` - 32-bit signed integer
- `DataType.UInt` - 32-bit unsigned integer
- `DataType.Long` - 64-bit signed integer
- `DataType.ULong` - 64-bit unsigned integer
- `DataType.Bool` - Boolean value
- `DataType.Char` - Single Unicode character
- `DataType.String` - Variable-length string
- `DataType.Float` - Single-precision floating point
- `DataType.Double` - Double-precision floating point
- `DataType.Decimal` - High-precision decimal number
- `DataType.DateTime` - Date and time

## Thread Safety & ACID Properties

### Thread Safety

All data modification operations (Insert, Update, Delete) are thread-safe and use reader-writer locks to ensure data consistency. Multiple threads can safely:

- Read data concurrently
- Insert data concurrently  
- Perform mixed read/write operations
- Execute independent transactions concurrently

### ACID Guarantees

The database provides full ACID transaction support:

- **Atomicity**: All operations in a transaction either succeed together or fail together
- **Consistency**: Database remains in a valid state before and after transactions
- **Isolation**: Concurrent transactions are isolated from each other (implemented via locking)
- **Durability**: Committed transactions are persisted via Write-Ahead Logging (WAL) and survive crashes

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

### Transaction Management

- **Write-Ahead Logging (WAL)**: All modifications are logged before being applied
- **Transaction isolation**: Reader-writer locks ensure transaction isolation
- **Automatic recovery**: Replays committed transactions from WAL on startup
- **Rollback support**: Uncommitted transactions are rolled back using undo operations
- **Checkpoint mechanism**: Marks points where all data has been flushed to disk

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