namespace MiniDatabaseEngine.Transaction;

/// <summary>
/// Represents the type of operation in the Write-Ahead Log
/// </summary>
public enum WALOperationType
{
    BeginTransaction,
    Insert,
    Update,
    Delete,
    Commit,
    Rollback,
    Checkpoint
}

/// <summary>
/// Represents an entry in the Write-Ahead Log
/// </summary>
public class WALEntry
{
    public long TransactionId { get; set; }
    public WALOperationType OperationType { get; set; }
    public string TableName { get; set; } = string.Empty;
    public object? Key { get; set; }
    public byte[]? OldValue { get; set; }
    public byte[]? NewValue { get; set; }
    public long Timestamp { get; set; }
    public long SequenceNumber { get; set; }

    public WALEntry()
    {
        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    /// <summary>
    /// Serialize the WAL entry to bytes
    /// </summary>
    public byte[] Serialize()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(TransactionId);
        writer.Write((int)OperationType);
        writer.Write(TableName ?? string.Empty);
        
        // Write key
        if (Key != null)
        {
            writer.Write(true);
            WriteObject(writer, Key);
        }
        else
        {
            writer.Write(false);
        }

        // Write OldValue
        if (OldValue != null)
        {
            writer.Write(true);
            writer.Write(OldValue.Length);
            writer.Write(OldValue);
        }
        else
        {
            writer.Write(false);
        }

        // Write NewValue
        if (NewValue != null)
        {
            writer.Write(true);
            writer.Write(NewValue.Length);
            writer.Write(NewValue);
        }
        else
        {
            writer.Write(false);
        }

        writer.Write(Timestamp);
        writer.Write(SequenceNumber);

        return ms.ToArray();
    }

    /// <summary>
    /// Deserialize a WAL entry from bytes
    /// </summary>
    public static WALEntry Deserialize(byte[] data)
    {
        using var ms = new MemoryStream(data);
        using var reader = new BinaryReader(ms);

        var entry = new WALEntry
        {
            TransactionId = reader.ReadInt64(),
            OperationType = (WALOperationType)reader.ReadInt32(),
            TableName = reader.ReadString()
        };

        // Read key
        if (reader.ReadBoolean())
        {
            entry.Key = ReadObject(reader);
        }

        // Read OldValue
        if (reader.ReadBoolean())
        {
            int oldValueLength = reader.ReadInt32();
            entry.OldValue = reader.ReadBytes(oldValueLength);
        }

        // Read NewValue
        if (reader.ReadBoolean())
        {
            int newValueLength = reader.ReadInt32();
            entry.NewValue = reader.ReadBytes(newValueLength);
        }

        entry.Timestamp = reader.ReadInt64();
        entry.SequenceNumber = reader.ReadInt64();

        return entry;
    }

    private static void WriteObject(BinaryWriter writer, object obj)
    {
        switch (obj)
        {
            case int i:
                writer.Write((byte)0);
                writer.Write(i);
                break;
            case long l:
                writer.Write((byte)1);
                writer.Write(l);
                break;
            case string s:
                writer.Write((byte)2);
                writer.Write(s);
                break;
            case double d:
                writer.Write((byte)3);
                writer.Write(d);
                break;
            case float f:
                writer.Write((byte)4);
                writer.Write(f);
                break;
            case bool b:
                writer.Write((byte)5);
                writer.Write(b);
                break;
            case byte bt:
                writer.Write((byte)6);
                writer.Write(bt);
                break;
            case short sh:
                writer.Write((byte)7);
                writer.Write(sh);
                break;
            case uint ui:
                writer.Write((byte)8);
                writer.Write(ui);
                break;
            case ulong ul:
                writer.Write((byte)9);
                writer.Write(ul);
                break;
            case sbyte sb:
                writer.Write((byte)10);
                writer.Write(sb);
                break;
            case ushort us:
                writer.Write((byte)11);
                writer.Write(us);
                break;
            case char c:
                writer.Write((byte)12);
                writer.Write(c);
                break;
            case decimal dec:
                writer.Write((byte)13);
                writer.Write(dec);
                break;
            case DateTime dt:
                writer.Write((byte)14);
                writer.Write(dt.ToBinary());
                break;
            default:
                throw new NotSupportedException($"Type {obj.GetType()} not supported for key serialization");
        }
    }

    private static object ReadObject(BinaryReader reader)
    {
        byte typeCode = reader.ReadByte();
        return typeCode switch
        {
            0 => reader.ReadInt32(),
            1 => reader.ReadInt64(),
            2 => reader.ReadString(),
            3 => reader.ReadDouble(),
            4 => reader.ReadSingle(),
            5 => reader.ReadBoolean(),
            6 => reader.ReadByte(),
            7 => reader.ReadInt16(),
            8 => reader.ReadUInt32(),
            9 => reader.ReadUInt64(),
            10 => reader.ReadSByte(),
            11 => reader.ReadUInt16(),
            12 => reader.ReadChar(),
            13 => reader.ReadDecimal(),
            14 => DateTime.FromBinary(reader.ReadInt64()),
            _ => throw new NotSupportedException($"Type code {typeCode} not supported")
        };
    }
}
