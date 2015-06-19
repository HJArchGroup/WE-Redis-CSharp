using System.Text;
using System.Collections.Generic;

namespace ServiceStack.Cluster
{
    public class UTF8String
    {
        public static byte[] ToBytes(string s)
        {
            return Encoding.UTF8.GetBytes(s);
        }

        public static byte[][] ToBytesArray(string[] s)
        {
            var bytes = new byte[s.Length][];
            for (var i = 0; i < s.Length; i++)
            {
                var item = s[i];
                bytes[i] = (item != null) ? UTF8String.ToBytes(item) : new byte[0];
            }
            return bytes;
        }

        public static byte[][] ToBytesArray(List<string> s)
        {
            var bytes = new byte[s.Count][];
            for (var i = 0; i < s.Count; ++ i)
            {
                var item = s[i];
                bytes[i] = (item != null) ? UTF8String.ToBytes(item) : new byte[0];
            }
            return bytes;
        }

        public static string ToString(byte[] bytes)
        {
            return (bytes == null || bytes.Length <= 0) ? null : Encoding.UTF8.GetString(bytes, 0, bytes.Length);
        }

        public static string[] ToStringArray(byte[][] bytes)
        {
            var strings = new string[bytes.GetLength(0)];
            for (var i = 0; i < bytes.Length; i++)
            {
                strings[i] = (bytes[i] != null) ? UTF8String.ToString(bytes[i]) : null;
            }
            return strings;
        }

        public static List<string> ToStringList(byte[][] bytes)
        {
            if (bytes == null)
            {
                return null;
            }

            var results = new List<string>();
            foreach (var item in bytes)
            {
                results.Add(UTF8String.ToString(item));
            }
            return results;
        }
    }
}
