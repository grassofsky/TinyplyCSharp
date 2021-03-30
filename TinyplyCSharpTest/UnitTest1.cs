using System;
using System.IO;
using System.Diagnostics;
using System.Collections.Generic;
using NUnit.Framework;
using TinyplyCSharp;

namespace TinyplyCSharpTest
{
    public class Tests
    {
        private string GetFilenameWithoutExtension(string path)
        {
            return System.IO.Path.GetFileNameWithoutExtension(path);
        }

        private void TranscodePlyFile(PlyFile file, string filepath)
        {
            var filename = GetFilenameWithoutExtension(filepath);

            using (FileStream outStream = File.Open(filename + "-transcode-binary.ply", FileMode.Create))
            {
                file.Write(outStream, true);
            }

            using (FileStream outStream = File.Open(filename + "-transcode-ascii.ply", FileMode.Create))
            {
                file.Write(outStream, false);
            }
        }

        private bool ParsePlyFile(string filepath)
        {
            var contents = File.ReadAllBytes(filepath);
            var sizemb = contents.Length * 1e-6;

            PlyFile file = new PlyFile();
            var ms = new MemoryStream(contents);
            bool headerResult = file.ParseHeader(ms);

            // All ply files are required to have a vertex element
            Dictionary<string, PlyData> vertexElement = new Dictionary<string, PlyData>();

            Console.WriteLine("Testing: " + filepath + " - filetype: " + (file.IsBinaryFile() ? "binary" : "ascii"));
            Assert.IsTrue(file.GetElements().Count > 0);

            string likelyFacePropertyName = null;
            // Extract a fat vertex structure (will likely include more than xyz)
            foreach (var e in file.GetElements())
            {
                if (e.Name == "vertex")
                {
                    Assert.IsTrue(e.Properties.Count > 0);
                    foreach (var p in e.Properties)
                    {
                        try
                        {
                            vertexElement.Add(p.Name, file.RequestPropertiesFromElement(e.Name, new List<string> { p.Name }));
                        }
                        catch
                        {

                        }
                    }
                }

                // Heuristic...
                if (e.Name == "face" || e.Name == "tristrips")
                {
                    for (int i = 0; i < 1; ++i)
                    {
                        likelyFacePropertyName = e.Properties[i].Name;
                    }
                }
            }

            PlyData faces, tripstrip;
            if (!string.IsNullOrEmpty(likelyFacePropertyName))
            {
                try
                {
                    faces = file.RequestPropertiesFromElement("face", new List<string> { likelyFacePropertyName }, 0);
                }
                catch { }

                try
                {
                    tripstrip = file.RequestPropertiesFromElement("tristrips", new List<string> { likelyFacePropertyName }, 0);
                }
                catch { }
            }

            Stopwatch sw = new Stopwatch();
            sw.Start();
            file.Read(ms);
            sw.Stop();
            Console.WriteLine("\tparsing " + sizemb + "mb in " + sw.ElapsedMilliseconds + " ms");

            foreach (var p in vertexElement)
            {
                Assert.IsTrue(p.Value.Count > 0);
                foreach (var e in file.GetElements())
                {
                    foreach (var prop in e.Properties)
                    {
                        if (e.Name == "vertex" && prop.Name == p.Key)
                        {
                            Assert.IsTrue(e.Size == p.Value.Count);
                        }
                    }
                }
            }

            sw.Restart();
            TranscodePlyFile(file, filepath);
            sw.Stop();

            Console.WriteLine("\ttranscoded in " + sw.ElapsedMilliseconds + " ms");


            return headerResult;
        }

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void TestImportingConformance()
        {
            ParsePlyFile("../../../../Assets/bunny.ply");
            ParsePlyFile("../../../../Assets/elephant.ply");
            ParsePlyFile("../../../../Assets/icosahedron.ply");
            ParsePlyFile("../../../../Assets/icosahedron_ascii.ply");
            ParsePlyFile("../../../../Assets/sofa.ply");
            ParsePlyFile("../../../../Assets/sofa_ascii.ply");
        }
    }
}