using System;
using System.Collections.Generic;
using System.IO;
using TinyplyCSharp;

namespace TinyplyCSharpExample
{
    [Serializable]
    public struct float2
    {
        public float2(float x, float y)
        {
            X = x;
            Y = y;
        }
        public float X, Y;
    };
    [Serializable]
    public struct float3
    {
        public float3(float x, float y, float z)
        {
            X = x;
            Y = y;
            Z = z;
        }
        public float X, Y, Z;
    };

    [Serializable]
    public struct int3
    {
        public int3(int x, int y, int z)
        {
            X = x;
            Y = y;
            Z = z;
        }
        public int X, Y, Z;
    };

    [Serializable]
    public struct int4
    {
        public int4(int x, int y, int z, int w)
        {
            X = x;
            Y = y;
            Z = z;
            W = w;
        }
        public int X, Y, Z, W;
    };

    [Serializable]
    public class Geometry
    {
        public List<float> vertices = new List<float>();
        public List<float> normals = new List<float>();
        public List<float> texcoords = new List<float>();
        public List<int> triangles = new List<int>();
    };

    struct CubeVertex
    {
        public CubeVertex(float3 position, float3 normal, float2 texCoord)
        {
            Position = position;
            Normal = normal;
            TexCoord = texCoord;
        }
        public float3 Position;
        public float3 Normal;
        public float2 TexCoord;
    }


    class Program
    {
        static Geometry MakeCubeGeometry()
        {
            CubeVertex[] verts =
            {
                new CubeVertex(new float3(-1,-1,-1), new float3(-1,0,0), new float2(0,0)),
                new CubeVertex(new float3(-1, -1, +1), new float3(-1, 0, 0), new float2(1,0)),
                new CubeVertex(new float3(-1, +1, +1), new float3(-1, 0, 0 ), new float2( 1, 1 )),
                new CubeVertex(new float3(-1,1,-1), new float3(-1,0,0), new float2(0,1)),

                new CubeVertex(new float3(1,-1,1), new float3(1, 0, 0), new float2(0, 0)),
                new CubeVertex(new float3(1,-1,-1), new float3(1,0,0), new float2(1,0)),
                new CubeVertex(new float3(1,1,-1), new float3(1,0,0), new float2(1,1)),
                new CubeVertex(new float3(1,1,1), new float3(1,0,0), new float2(0,1)),

                new CubeVertex(new float3(-1,-1,-1), new float3(0, -1,0), new float2(0,0)),
                new CubeVertex(new float3(1,-1,-1), new float3(0, -1, 0), new float2(1,0)),
                new CubeVertex(new float3(1,-1,1), new float3(0, -1, 0), new float2(1,1)),
                new CubeVertex(new float3(-1,-1,1), new float3(0, -1, 0), new float2(0, 1)),

                new CubeVertex(new float3(1,1,-1), new float3(0,1,0), new float2(0,0)),
                new CubeVertex(new float3(-1,1,-1), new float3(0,1,0), new float2(1,0)),
                new CubeVertex(new float3(-1,1,1), new float3(0,1,0), new float2(1,1)),
                new CubeVertex(new float3(1,1,1), new float3(0,1,0), new float2(0,1)),

                new CubeVertex(new float3(-1,-1,-1), new float3(0,0,-1), new float2(0,0)),
                new CubeVertex(new float3(-1,1,-1), new float3(0,0,-1), new float2(1,0)),
                new CubeVertex(new float3(1,1,-1), new float3(0,0,-1), new float2(1,1)),
                new CubeVertex(new float3(1,-1,-1), new float3(0,0,-1), new float2(0,1)),

                new CubeVertex(new float3(-1,1,1), new float3(0,0,1), new float2(0,0)),
                new CubeVertex(new float3(-1,-1,1), new float3(0,0,1), new float2(1,0)),
                new CubeVertex(new float3(1,-1,1), new float3(0,0,1), new float2(1,1)),
                new CubeVertex(new float3(1,1,1), new float3(0,0,1), new float2(0,1))
            };

            int4[] quads =
            {
                new int4(0,1,2,3),
                new int4(4,5,6,7),
                new int4(8,9,10,11),
                new int4(12,13,14,15),
                new int4(16,17,18,19),
                new int4(20,21,22,23)
            };

            Geometry cube = new Geometry();

            foreach (var q in quads)
            {
                cube.triangles.Add(q.X);
                cube.triangles.Add(q.Y);
                cube.triangles.Add(q.Z);

                cube.triangles.Add(q.X);
                cube.triangles.Add(q.Z);
                cube.triangles.Add(q.W);
            }

            for (int i = 0; i < 24; ++i)
            {
                cube.vertices.Add(verts[i].Position.X);
                cube.vertices.Add(verts[i].Position.Y);
                cube.vertices.Add(verts[i].Position.Z);

                cube.normals.Add(verts[i].Normal.X);
                cube.normals.Add(verts[i].Normal.Y);
                cube.normals.Add(verts[i].Normal.Z);

                cube.texcoords.Add(verts[i].TexCoord.X);
                cube.texcoords.Add(verts[i].TexCoord.Y);
            }

            return cube;
        }

        static void WritePlyExample(string filename)
        {
            Geometry cube = MakeCubeGeometry();

            {
                PlyFile cubeFile = new PlyFile();


                byte[] verticesbytes = new byte[cube.vertices.Count * sizeof(float)];
                System.Buffer.BlockCopy(cube.vertices.ToArray(), 0, verticesbytes, 0, verticesbytes.Length);
                cubeFile.AddPropertiesToElement("vertex", new List<string> { "x", "y", "z" },
                    TinyplyCSharp.Type.FLOAT32, cube.vertices.Count / 3, verticesbytes, TinyplyCSharp.Type.INVALID, 0);

                byte[] normalbytes = new byte[cube.normals.Count * sizeof(float)];
                System.Buffer.BlockCopy(cube.normals.ToArray(), 0, normalbytes, 0, normalbytes.Length);
                cubeFile.AddPropertiesToElement("vertex", new List<string> { "nx", "ny", "nz" },
                    TinyplyCSharp.Type.FLOAT32, cube.normals.Count / 3, normalbytes, TinyplyCSharp.Type.INVALID, 0);

                byte[] trianglebytes = new byte[cube.triangles.Count * sizeof(int)];
                System.Buffer.BlockCopy(cube.triangles.ToArray(), 0, trianglebytes, 0, trianglebytes.Length);
                cubeFile.AddPropertiesToElement("face", new List<string> { "vertex_indices" },
                    TinyplyCSharp.Type.UINT32, cube.triangles.Count / 3, trianglebytes, TinyplyCSharp.Type.UINT8, 3);

                cubeFile.GetComments().Add("generated by tinyply 2.3");

                using (FileStream outStream = File.Open(filename + "-ascii.ply", FileMode.Create))
                {
                    cubeFile.Write(outStream, false);
                }

                using (FileStream outStream = File.Open(filename + "-binary.ply", FileMode.Create))
                {
                    cubeFile.Write(outStream, true);
                }

            }
        }

        static public void ReadPlyFile(string filePath, bool preloadIntoMemory = true)
        {
            {
                byte[] contents = File.ReadAllBytes(filePath);
                var ms = new MemoryStream(contents);
                PlyFile file = new PlyFile();
                file.ParseHeader(ms);
                Console.WriteLine("[ply_header] Type: " + (file.IsBinaryFile() ? "binary" : "ascii"));
                foreach (var comment in file.GetComments())
                {
                    Console.WriteLine("\t[ply_header] Comment: " + comment);
                }

                foreach (var info in file.GetInfo())
                {
                    Console.WriteLine("\t[ply_header] Info: " + info);
                }

                foreach (var element in file.GetElements())
                {
                    Console.WriteLine("\t[ply_header] element: " + element.Name + "(" + element.Size + ")");
                    foreach (var p in element.Properties)
                    {
                        Console.Write("\t[ply_header] \tproperty: " + p.Name + " (type=" + PlyHelper.PropertyTable[p.PropertyType].Str + ")");
                        if (p.IsList)
                        {
                            Console.Write(" (list_type=" + PlyHelper.PropertyTable[p.ListType].Str + ")");
                        }
                        Console.WriteLine();
                    }
                }

                PlyData vertices = file.RequestPropertiesFromElement("vertex", new List<string> { "x", "y", "z" });
                PlyData normals = file.RequestPropertiesFromElement("vertex", new List<string> { "nx", "ny", "nz" });
                PlyData faces = file.RequestPropertiesFromElement("face", new List<string> { "vertex_indices" });
                file.Read(ms);

                if (vertices != null)
                {
                    Console.WriteLine("\tRead " + vertices.Count + " total vertices");

                }
                if (normals != null)
                {
                    Console.WriteLine("\tRead " + normals.Count + " total vertex normals");
                }
                if (faces != null)
                {
                    Console.WriteLine("\tRead " + faces.Count + " total faces (triangles)");
                }
            }
        }

        static void Main(string[] args)
        {
            WritePlyExample("example_cube");
            ReadPlyFile("example_cube-ascii.ply");
            ReadPlyFile("example_cube-binary.ply");

            Console.ReadKey();
        }
    }
}
