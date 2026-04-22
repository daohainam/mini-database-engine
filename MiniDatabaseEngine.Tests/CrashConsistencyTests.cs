using Xunit;

namespace MiniDatabaseEngine.Tests;

public class CrashConsistencyTests
{
    [Fact]
    public void NonTransactional_Unflushed_Write_Is_Not_In_Crash_Snapshot_On_Restart()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_crash_unflushed_{Guid.NewGuid()}.mde");
        var snapshotPath = Path.Combine(Path.GetTempPath(), $"test_crash_unflushed_snapshot_{Guid.NewGuid()}.mde");
        var walPath = Path.ChangeExtension(dbPath, ".wal");
        var snapshotWalPath = Path.ChangeExtension(snapshotPath, ".wal");

        try
        {
            using (var db = new Database(dbPath))
            {
                var table = db.CreateTable("Users",
                [
                    new("Id", DataType.Int, false),
                    new("Name", DataType.String, false)
                ], "Id");

                var row = new DataRow(table.Schema);
                row["Id"] = 1;
                row["Name"] = "Alice";
                db.Insert("Users", row);

                File.Copy(dbPath, snapshotPath, overwrite: true);
                if (File.Exists(walPath))
                    File.Copy(walPath, snapshotWalPath, overwrite: true);
            }

            using var restarted = new Database(snapshotPath);
            Assert.True(restarted.TableExists("Users"));
            var users = restarted.GetTable("Users");
            var alice = users.SelectByKey(1);
            Assert.Null(alice);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(walPath);
            DeleteIfExists(snapshotPath);
            DeleteIfExists(snapshotWalPath);
        }
    }

    [Fact]
    public void NonTransactional_Flushed_Write_Survives_Restart()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_crash_flushed_{Guid.NewGuid()}.mde");
        var snapshotPath = Path.Combine(Path.GetTempPath(), $"test_crash_flushed_snapshot_{Guid.NewGuid()}.mde");
        var walPath = Path.ChangeExtension(dbPath, ".wal");
        var snapshotWalPath = Path.ChangeExtension(snapshotPath, ".wal");

        try
        {
            using (var db = new Database(dbPath))
            {
                var table = db.CreateTable("Users",
                [
                    new("Id", DataType.Int, false),
                    new("Name", DataType.String, false)
                ], "Id");

                var row = new DataRow(table.Schema);
                row["Id"] = 1;
                row["Name"] = "Alice";
                db.Insert("Users", row);
                db.Flush();

                File.Copy(dbPath, snapshotPath, overwrite: true);
                if (File.Exists(walPath))
                    File.Copy(walPath, snapshotWalPath, overwrite: true);
            }

            using var restarted = new Database(snapshotPath);
            Assert.True(restarted.TableExists("Users"));
            var users = restarted.GetTable("Users");
            var alice = users.SelectByKey(1);
            Assert.NotNull(alice);
            Assert.Equal("Alice", alice!["Name"]);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(walPath);
            DeleteIfExists(snapshotPath);
            DeleteIfExists(snapshotWalPath);
        }
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
