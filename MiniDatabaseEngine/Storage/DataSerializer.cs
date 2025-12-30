using System.Text;

namespace MiniDatabaseEngine.Storage;

/// <summary>
/// Handles serialization and deserialization of data types to/from byte arrays
/// </summary>
public static class DataSerializer
{
    public static byte[] Serialize(object? value, DataType dataType)
    {
        if (value == null)
            return new byte[] { 0 }; // Null marker
            
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        
        writer.Write((byte)1); // Not null marker
        
        switch (dataType)
        {
            case DataType.Byte:
                writer.Write((byte)value);
                break;
            case DataType.Int:
                writer.Write((int)value);
                break;
            case DataType.Long:
                writer.Write((long)value);
                break;
            case DataType.Bool:
                writer.Write((bool)value);
                break;
            case DataType.String:
                var str = (string)value;
                writer.Write(str);
                break;
            case DataType.Float:
                writer.Write((float)value);
                break;
            case DataType.Double:
                writer.Write((double)value);
                break;
            case DataType.Decimal:
                writer.Write((decimal)value);
                break;
            case DataType.DateTime:
                writer.Write(((DateTime)value).ToBinary());
                break;
            default:
                throw new NotSupportedException($"Data type {dataType} not supported");
        }
        
        return ms.ToArray();
    }
    
    public static object? Deserialize(byte[] data, DataType dataType)
    {
        using var ms = new MemoryStream(data);
        using var reader = new BinaryReader(ms);
        
        var isNull = reader.ReadByte() == 0;
        if (isNull)
            return null;
            
        switch (dataType)
        {
            case DataType.Byte:
                return reader.ReadByte();
            case DataType.Int:
                return reader.ReadInt32();
            case DataType.Long:
                return reader.ReadInt64();
            case DataType.Bool:
                return reader.ReadBoolean();
            case DataType.String:
                return reader.ReadString();
            case DataType.Float:
                return reader.ReadSingle();
            case DataType.Double:
                return reader.ReadDouble();
            case DataType.Decimal:
                return reader.ReadDecimal();
            case DataType.DateTime:
                return DateTime.FromBinary(reader.ReadInt64());
            default:
                throw new NotSupportedException($"Data type {dataType} not supported");
        }
    }
    
    public static int Compare(object? a, object? b, DataType dataType)
    {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        
        switch (dataType)
        {
            case DataType.Byte:
                return ((byte)a).CompareTo((byte)b);
            case DataType.Int:
                return ((int)a).CompareTo((int)b);
            case DataType.Long:
                return ((long)a).CompareTo((long)b);
            case DataType.Bool:
                return ((bool)a).CompareTo((bool)b);
            case DataType.String:
                return string.Compare((string)a, (string)b, StringComparison.Ordinal);
            case DataType.Float:
                return ((float)a).CompareTo((float)b);
            case DataType.Double:
                return ((double)a).CompareTo((double)b);
            case DataType.Decimal:
                return ((decimal)a).CompareTo((decimal)b);
            case DataType.DateTime:
                return ((DateTime)a).CompareTo((DateTime)b);
            default:
                throw new NotSupportedException($"Data type {dataType} not supported");
        }
    }
}
