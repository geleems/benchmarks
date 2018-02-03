using System.Linq;
using System.Text;

namespace PostgreSql.Native
{
    internal static class LibpqExtensions
    {
        internal static byte[] ToByteArray(this string[] values)
        {
            var allParameterBytes = values.Select(s => Encoding.UTF8.GetBytes(s)).ToArray();
            var bufferSize = allParameterBytes.Sum(x => x.Length + 1); // +1 as each string is NULL terminated

            var buffer = new byte[bufferSize];

            var index = 0;
            for (var i = 0; i < allParameterBytes.Length; i++)
            {
                allParameterBytes[i].CopyTo(buffer, index);
                index += allParameterBytes[i].Length;
                buffer[index] = 0; // NULL terminates the string
                index++;
            }

            return buffer;
        }
    }
}
