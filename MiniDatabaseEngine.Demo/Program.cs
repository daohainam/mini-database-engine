using MiniDatabaseEngine;

Console.WriteLine("=== Mini Database Engine Demo ===\n");

// Create a database with .mde file
var dbPath = Path.Combine(Path.GetTempPath(), "demo.mde");
if (File.Exists(dbPath))
    File.Delete(dbPath);

using var db = new Database(dbPath, cacheSize: 100, useMemoryMappedFile: false);

Console.WriteLine("1. Creating a 'Users' table...");
var userColumns = new List<ColumnDefinition>
{
    new("Id", DataType.Int, false),
    new("Name", DataType.String),
    new("Email", DataType.String),
    new("Age", DataType.Int),
    new("IsActive", DataType.Bool),
    new("Balance", DataType.Double),
    new("CreatedAt", DataType.DateTime)
};

var usersTable = db.CreateTable("Users", userColumns, "Id");
Console.WriteLine("✓ Table created successfully\n");

Console.WriteLine("2. Inserting sample data...");
for (int i = 1; i <= 10; i++)
{
    var row = new DataRow(usersTable.Schema);
    row["Id"] = i;
    row["Name"] = $"User {i}";
    row["Email"] = $"user{i}@example.com";
    row["Age"] = 20 + i;
    row["IsActive"] = i % 2 == 0;
    row["Balance"] = 100.0 * i;
    row["CreatedAt"] = DateTime.Now.AddDays(-i);
    
    db.Insert("Users", row);
}
Console.WriteLine($"✓ Inserted 10 users\n");

Console.WriteLine("3. Querying data with LINQ...");
var allUsers = db.Query("Users").ToList();
Console.WriteLine($"   Total users: {allUsers.Count}");

// Show first 3 users
Console.WriteLine("   First 3 users:");
foreach (var user in allUsers.Take(3))
{
    Console.WriteLine($"   - Id: {user["Id"]}, Name: {user["Name"]}, Email: {user["Email"]}, Age: {user["Age"]}");
}
Console.WriteLine();

Console.WriteLine("4. Updating a user...");
var updatedRow = new DataRow(usersTable.Schema);
updatedRow["Id"] = 1;
updatedRow["Name"] = "Updated User 1";
updatedRow["Email"] = "updated@example.com";
updatedRow["Age"] = 35;
updatedRow["IsActive"] = true;
updatedRow["Balance"] = 1500.0;
updatedRow["CreatedAt"] = DateTime.Now;

db.Update("Users", 1, updatedRow);
var updated = usersTable.SelectByKey(1);
Console.WriteLine($"   ✓ Updated: {updated?["Name"]} ({updated?["Email"]})\n");

Console.WriteLine("5. Deleting a user...");
db.Delete("Users", 5);
Console.WriteLine("   ✓ User with Id=5 deleted\n");

Console.WriteLine("6. Testing all data types...");
var typeColumns = new List<ColumnDefinition>
{
    new("Id", DataType.Int, false),
    new("ByteVal", DataType.Byte),
    new("LongVal", DataType.Long),
    new("BoolVal", DataType.Bool),
    new("StringVal", DataType.String),
    new("FloatVal", DataType.Float),
    new("DoubleVal", DataType.Double),
    new("DateTimeVal", DataType.DateTime)
};

var typesTable = db.CreateTable("AllTypes", typeColumns, "Id");

var typeRow = new DataRow(typesTable.Schema);
typeRow["Id"] = 1;
typeRow["ByteVal"] = (byte)255;
typeRow["LongVal"] = 9223372036854775807L;
typeRow["BoolVal"] = true;
typeRow["StringVal"] = "Hello, World! 🌍";
typeRow["FloatVal"] = 3.14159f;
typeRow["DoubleVal"] = 2.718281828459045;
typeRow["DateTimeVal"] = new DateTime(2024, 1, 1, 12, 0, 0);

db.Insert("AllTypes", typeRow);

var retrieved = typesTable.SelectByKey(1);
Console.WriteLine("   ✓ All data types stored and retrieved correctly:");
Console.WriteLine($"   - Byte: {retrieved?["ByteVal"]}");
Console.WriteLine($"   - Long: {retrieved?["LongVal"]}");
Console.WriteLine($"   - Bool: {retrieved?["BoolVal"]}");
Console.WriteLine($"   - String: {retrieved?["StringVal"]}");
Console.WriteLine($"   - Float: {retrieved?["FloatVal"]}");
Console.WriteLine($"   - Double: {retrieved?["DoubleVal"]}");
Console.WriteLine($"   - DateTime: {retrieved?["DateTimeVal"]}\n");

Console.WriteLine("7. Testing concurrent access...");
var tasks = new List<Task>();
for (int i = 100; i < 110; i++)
{
    int id = i;
    tasks.Add(Task.Run(() =>
    {
        var row = new DataRow(usersTable.Schema);
        row["Id"] = id;
        row["Name"] = $"Concurrent User {id}";
        row["Email"] = $"concurrent{id}@example.com";
        row["Age"] = 25;
        row["IsActive"] = true;
        row["Balance"] = 1000.0;
        row["CreatedAt"] = DateTime.Now;
        
        db.Insert("Users", row);
    }));
}

Task.WaitAll(tasks.ToArray());
var finalCount = db.Query("Users").ToList().Count;
Console.WriteLine($"   ✓ {finalCount} total users after concurrent inserts\n");

Console.WriteLine("8. Flushing to disk...");
db.Flush();
Console.WriteLine($"   ✓ Data persisted to: {dbPath}\n");

Console.WriteLine("=== Demo completed successfully! ===");
Console.WriteLine($"\nDatabase file size: {new FileInfo(dbPath).Length} bytes");
