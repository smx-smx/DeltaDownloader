using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using MsDelta;
using System.Net.Http;
using System.Collections.Concurrent;

namespace DeltaDownloader
{
    internal class Program
    {
        private const uint PAGE_SIZE = 0x1000;
        private static readonly HttpClientHandler s_clientHandler = new HttpClientHandler() { AllowAutoRedirect = false };
        private static readonly HttpClient s_client = new HttpClient(s_clientHandler);
        private static readonly ConcurrentBag<string> s_urls = new ConcurrentBag<string>();
        private static readonly ConcurrentBag<(string, string, DeltaFile)> s_files = new ConcurrentBag<(string, string, DeltaFile)>();
        private static async Task<bool> SymbolUrlValid(string url)
        {
            var request = new HttpRequestMessage(HttpMethod.Head, url);
            while (true)
            {
                var response = await s_client.SendAsync(request);
                var intStatus = (int)response.StatusCode;
                if (intStatus >= 500 && intStatus < 600)
                {
                    await Task.Delay(100);
                    continue;
                }
                return response.StatusCode == System.Net.HttpStatusCode.Found;
            }
        }

        private static ulong GetMappedSize(ulong size)
        {
            const ulong PAGE_MASK = (PAGE_SIZE - 1);
            var page = size & ~PAGE_MASK;
            if (page == size) return page;
            return page + PAGE_SIZE;
        }

        private static string CreateUrl(string filename, uint TimeDateStamp, uint SizeOfImage)
        {
            return string.Format("https://msdl.microsoft.com/download/symbols/{0}/{1:x8}{2:x}/{0}", filename, TimeDateStamp, SizeOfImage);
        }

        private static void ParseDelta(string path, string dir)
        {
            try
            {
                var bytes = File.ReadAllBytes(path);
                var delta = new DeltaFile(bytes);
                var filename = Path.GetFileName(path);
                // https://msdl.microsoft.com/download/symbols/cdboot_noprompt.efi/{0:X8}{1:X}/cdboot_noprompt.efi, TimeDateStamp, SizeOfImage
                if ((delta.Code & FileTypeCode.Raw) != 0) return;
                if (delta.FileTypeHeader?.RiftTable == null) return;
                s_files.Add((dir, filename, delta));
            } catch { }
        }

        private static async Task<string> GetUrlFromDelta((string, string, DeltaFile) deltaPair)
        {
            var filename = deltaPair.Item2;
            var delta = deltaPair.Item3;

            var timeDateStamp = delta.FileTypeHeader.TimeStamp;
            // We use the rift table (VirtualAddress,PointerToRawData pairs for each section) and the target file size to calculate the SizeOfImage.
            var lastSection = delta.FileTypeHeader.RiftTable.Last();
            var lastSectionAndSignatureSize = delta.TargetSize - lastSection.Value;
            var lastSectionMapped = lastSection.Key;
            var lastSectionAndSignatureMappedSize = GetMappedSize(lastSectionMapped + lastSectionAndSignatureSize);

            uint sizeOfImage = (uint)lastSectionAndSignatureMappedSize;
            uint lowestSizeOfImage = (uint)lastSectionMapped + PAGE_SIZE;

            var urls = new List<string>();
            var tasks = new List<Task<bool>>();
            for (uint size = sizeOfImage; size >= lowestSizeOfImage; size -= PAGE_SIZE)
            {
                string url = CreateUrl(filename, timeDateStamp, size);
                urls.Add(url);
                tasks.Add(Task.Run(() => SymbolUrlValid(url)));
            }
            await Task.WhenAll(tasks);
            for (int i = 0; i < tasks.Count; i++)
            {
                if (tasks[i].Result) return urls[i];
            }
            throw new InvalidDataException();
        }

        static async Task ProcessDelta((string, string, DeltaFile) delta)
        {
            try
            {
                var url = await GetUrlFromDelta(delta);
                var sb = new StringBuilder(url);
                sb.AppendLine();
                sb.AppendFormat(" out={0}", delta.Item2);
                s_urls.Add(sb.ToString());
            }
            catch { }
        }

        static void Process(string directory, string parent, bool inDir = false)
        {
            try
            {
                foreach (var folder in Directory.EnumerateDirectories(directory))
                {
                    Process(folder, directory, inDir || folder.Substring(directory.Length + 1) == "f");
                }
                if (!inDir) return;
                foreach (var file in Directory.EnumerateFiles(directory))
                {
                    if (Path.GetExtension(file) == "mui") continue;
                    ParseDelta(file, parent);
                }
                Console.Write(".");
            } catch { }
        }

        static async Task SearchForDeltaFiles(string directory)
        {
            var tasks = new List<Task>();
            Console.Write("Searching for delta files");
            Process(directory, directory);
            Console.WriteLine();
            Console.WriteLine("Attempting to find {0} files on the Microsoft Symbol Server...", s_files.Count);
            foreach (var file in s_files)
            {
                tasks.Add(Task.Run(() => ProcessDelta(file)));
            }
            var whenAll = Task.WhenAll(tasks);
            while (true)
            {
                await Task.WhenAny(whenAll, Task.Delay(1000));
                if (whenAll.IsCompleted) break;
                var tasksCompletedCount = tasks.Where((t) => t.IsCompleted).Count();
                double percent = 100.0 * tasksCompletedCount / tasks.Count;
                Console.Write("{0}% complete ({1}/{2})\r", Math.Round(percent, 2), tasksCompletedCount, tasks.Count);
            }
            Console.WriteLine();

            var sb = new StringBuilder();
            foreach (var aria in s_urls)
            {
                sb.AppendLine(aria);
            }
            var ariaPath = Path.Combine(directory, "aria2.txt");
            File.WriteAllText(ariaPath, sb.ToString());
            Console.WriteLine("Written aria2 input file to {0}", ariaPath);
        }

        static string ExtractDeltaFileInformation(string path)
        {
            var sb = new StringBuilder();

            var bytes = File.ReadAllBytes(path);
            var delta = new DeltaFile(bytes);

            var hashStr = BitConverter.ToString(delta.Hash).Replace("-", "");
            var additionalHashStr = BitConverter.ToString(delta.AdditionalHash).Replace("-", "");

            sb.Append("### Header").AppendLine()
                .AppendFormat("FileTime: {0}", delta.FileTime).AppendLine()
                .AppendFormat("Version: {0}", delta.Version).AppendLine()
                .AppendFormat("Code: {0}", delta.Code).AppendLine()
                .AppendFormat("Flags: {0}", delta.Flags).AppendLine()
                .AppendFormat("TargetSize: {0}", delta.TargetSize).AppendLine()
                .AppendFormat("HashAlgorithm: {0}", delta.HashAlgorithm).AppendLine()
                .AppendFormat("Hash: {0}", hashStr).AppendLine()
                .AppendFormat("HeaderInfoSize: {0}", delta.HeaderInfoSize).AppendLine()
                .AppendFormat("IsPa31: {0}", delta.IsPa31).AppendLine()
                .AppendFormat("DeltaClientMinVersion: {0}", delta.DeltaClientMinVersion).AppendLine()
                .AppendFormat("AdditionalHash: {0}", additionalHashStr).AppendLine();

            var fileTypeHeader = delta.FileTypeHeader;
            if (fileTypeHeader == null)
            {
                return sb.ToString();
            }

            var riftTableStr =
                fileTypeHeader.RiftTable != null
                    ? string.Join(";", fileTypeHeader.RiftTable.Select(x => x.Key + "," + x.Value).ToArray())
                    : "(none)";

            sb.AppendLine().Append("### FileTypeHeader").AppendLine()
                .AppendFormat("ImageBase: {0}", fileTypeHeader.ImageBase).AppendLine()
                .AppendFormat("GlobalPointer: {0}", fileTypeHeader.GlobalPointer).AppendLine()
                .AppendFormat("TimeStamp: {0}", fileTypeHeader.TimeStamp).AppendLine()
                .AppendFormat("RiftTable: {0}", riftTableStr).AppendLine();

            var cliMetadata = fileTypeHeader.CliMetadata;
            if (cliMetadata == null)
            {
                return sb.ToString();
            }

            sb.AppendLine().Append("### CliMetadata").AppendLine()
                .AppendFormat("StartOffset: {0}", cliMetadata.m_StartOffset).AppendLine()
                .AppendFormat("Size: {0}", cliMetadata.m_Size).AppendLine()
                .AppendFormat("BaseRva: {0}", cliMetadata.m_BaseRva).AppendLine()
                .AppendFormat("StreamsNumber: {0}", cliMetadata.m_StreamsNumber).AppendLine()
                .AppendFormat("StreamHeadersOffset: {0}", cliMetadata.m_StreamHeadersOffset).AppendLine()
                .AppendFormat("StringsStreamOffset: {0}", cliMetadata.m_StringsStreamOffset).AppendLine()
                .AppendFormat("StringsStreamSize: {0}", cliMetadata.m_StringsStreamSize).AppendLine()
                .AppendFormat("USStreamOffset: {0}", cliMetadata.m_USStreamOffset).AppendLine()
                .AppendFormat("USStreamSize: {0}", cliMetadata.m_USStreamSize).AppendLine()
                .AppendFormat("BlobStreamOffset: {0}", cliMetadata.m_BlobStreamOffset).AppendLine()
                .AppendFormat("BlobStreamSize: {0}", cliMetadata.m_BlobStreamSize).AppendLine()
                .AppendFormat("GuidStreamOffset: {0}", cliMetadata.m_GuidStreamOffset).AppendLine()
                .AppendFormat("GuidStreamSize: {0}", cliMetadata.m_GuidStreamSize).AppendLine()
                .AppendFormat("TablesStreamOffset: {0}", cliMetadata.m_TablesStreamOffset).AppendLine()
                .AppendFormat("TablesStreamSize: {0}", cliMetadata.m_TablesStreamSize).AppendLine()
                .AppendFormat("LongStringsStream: {0}", cliMetadata.m_LongStringsStream).AppendLine()
                .AppendFormat("LongGuidStream: {0}", cliMetadata.m_LongGuidStream).AppendLine()
                .AppendFormat("LongBlobStream: {0}", cliMetadata.m_LongBlobStream).AppendLine()
                .AppendFormat("ValidTables: {0}", cliMetadata.m_ValidTables).AppendLine();

            return sb.ToString();
        }

        static void PrintDeltaFileInformation(string path)
        {
            Console.Write(ExtractDeltaFileInformation(path));
        }

        static int GenerateDeltaSubFolderInformation(string directory)
        {
            int filesCreated = 0;

            var files = Directory.GetFiles(directory);
            foreach (var file in files)
            {
                var informationFile = file + ".dd.txt";
                if (File.Exists(informationFile))
                {
                    throw new Exception(string.Format("{0} already exists", informationFile));
                }

                var information = ExtractDeltaFileInformation(file);
                File.WriteAllText(informationFile, information);
                filesCreated++;
            }

            foreach (var subdirectory in Directory.EnumerateDirectories(directory))
            {
                filesCreated += GenerateDeltaSubFolderInformation(subdirectory);
            }

            return filesCreated;
        }

        static void GenerateDeltaFolderInformation(string directory)
        {
            int filesCreated = 0;

            foreach (var folder in Directory.EnumerateDirectories(directory))
            {
                var fFolder = Path.Combine(folder, "f");
                if (Directory.Exists(fFolder))
                {
                    filesCreated += GenerateDeltaSubFolderInformation(fFolder);
                }
            }

            Console.WriteLine("{0} information files were created", filesCreated);
        }

        static async Task Main(string[] args)
        {
            if (args.Length == 1)
            {
                await SearchForDeltaFiles(args[0]);
            }
            else if (args.Length == 2 && args[0] == "/i")
            {
                PrintDeltaFileInformation(args[1]);
            }
            else if (args.Length == 2 && args[0] == "/g")
            {
                GenerateDeltaFolderInformation(args[1]);
            }
            else
            {
                Console.WriteLine("Usage: {0} <dir>", Path.GetFileName(System.Reflection.Assembly.GetEntryAssembly().Location));
                Console.WriteLine();
                Console.WriteLine("Given a folder containing delta compressed PE files from a Windows update package,");
                Console.WriteLine("uses the data in the delta compression header to search for the PE files on the MS symbol server.");
                Console.WriteLine();
                Console.WriteLine("{0} /i <file>", Path.GetFileName(System.Reflection.Assembly.GetEntryAssembly().Location));
                Console.WriteLine();
                Console.WriteLine("Given a delta compressed PE file from a Windows update package, print information about it.");
                Console.WriteLine();
                Console.WriteLine("{0} /g <dir>", Path.GetFileName(System.Reflection.Assembly.GetEntryAssembly().Location));
                Console.WriteLine();
                Console.WriteLine("Given a folder containing delta compressed PE files from a Windows update package,");
                Console.WriteLine("writes textual information near every delta file.");
            }
        }
    }
}
