using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

namespace Ma7MQ.Core.Broker
{
    public static class CompressionHelper
    {
        public static bool ShouldCompress(byte[] data, int minSize)
        {
            if (data == null || data.Length < minSize)
                return false;

            double entropy = CalculateEntropy(data);
            // Entropy threshold
            return entropy < 7.2;
        }

        private static double CalculateEntropy(byte[] data)
        {
            var freq = new Dictionary<byte, int>();
            foreach (var b in data)
            {
                if (!freq.ContainsKey(b))
                    freq[b] = 0;
                freq[b]++;
            }

            double entropy = 0;
            double length = data.Length;

            foreach (var count in freq.Values)
            {
                double p = count / length;
                entropy -= p * Math.Log(p, 2);
            }

            return entropy;
        }

        public static async Task<byte[]> CompressAsync(byte[] data)
        {
            using var output = new MemoryStream();
            using (var gZipStream = new GZipStream(output, CompressionMode.Compress))
            {
                await gZipStream.WriteAsync(data.AsMemory(0, data.Length));
            }
            return output.ToArray();
        }

        public static async Task<byte[]> DecompressAsync(byte[] data)
        {
            using var input = new MemoryStream(data);
            using var output = new MemoryStream();
            using (var gZipStream = new GZipStream(input, CompressionMode.Decompress))
            {
                await gZipStream.CopyToAsync(output);
            }
            return output.ToArray();
        }
    }
}
// Optimized entropy checks for faster checks
