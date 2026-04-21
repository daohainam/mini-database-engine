using System.Text;

namespace MiniDatabaseEngine;

internal static class BinaryDecodingGuards
{
    internal static string ReadBoundedString(BinaryReader reader, int maxByteLength, string fieldName)
    {
        int byteLength = Read7BitEncodedInt32(reader);
        if (byteLength < 0 || byteLength > maxByteLength)
            throw new InvalidDataException($"Invalid {fieldName} length: {byteLength}");

        var bytes = reader.ReadBytes(byteLength);
        if (bytes.Length != byteLength)
            throw new EndOfStreamException($"Incomplete {fieldName} payload.");

        return Encoding.UTF8.GetString(bytes);
    }

    internal static int Read7BitEncodedInt32(BinaryReader reader)
    {
        uint result = 0;
        int shift = 0;

        while (shift < 32)
        {
            byte b = reader.ReadByte();
            result |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                if (result > int.MaxValue)
                    throw new InvalidDataException("7-bit encoded value exceeds Int32 range.");
                return (int)result;
            }

            shift += 7;
        }

        throw new InvalidDataException("Invalid 7-bit encoded integer.");
    }
}
