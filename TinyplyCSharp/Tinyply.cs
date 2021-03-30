using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace TinyplyCSharp
{
    public enum Type
    {
        INVALID,
        INT8,
        UINT8,
        INT16,
        UINT16,
        INT32,
        UINT32,
        FLOAT32,
        FLOAT64
    }

    public class PropertyInfo
    {
        public PropertyInfo()
        { }
        public PropertyInfo(int stride, string str)
        {
            Stride = stride;
            Str = str;

        }
        public int Stride { get; set; }
        public string Str { get; set; }
    }

    public class Buffer
    {
        private byte[] _data = null;
        public Buffer()
        {

        }

        public Buffer(byte[] data)
        {
            _data = data;
        }

        public Buffer(int size)
        {
            _data = new byte[size];
        }

        public byte[] Get() { return _data; }
        public int SizeBytes() { return _data.Length; }
    }

    public class PlyData
    {
        public PlyData()
        {
            Buffer = null;
            Count = 0;
            IsList = false;
        }
        public Type T { get; set; }
        public Buffer Buffer { get; set; }
        public int Count { get; set; }
        public bool IsList { get; set; }
    }

    public class PlyProperty
    {
        public PlyProperty(BinaryReader istream)
        {
            string type = istream.ReadWord();
            if (type == "list")
            {
                string countType = istream.ReadWord();
                type = istream.ReadWord();
                ListType = PlyHelper.PropertyTypeFromString(countType);
                IsList = true;
            }
            PropertyType = PlyHelper.PropertyTypeFromString(type);
            Name = istream.ReadWord();
        }

        public PlyProperty(Type type, string name)
        {
            PropertyType = type;
            Name = name;
        }

        public PlyProperty(Type listType, Type propType, string name, List<int> listCounts)
        {
            Name = name;
            PropertyType = propType;
            IsList = true;
            ListType = listType;
            ListCounts = listCounts;
        }

        public string Name { get; set; }
        public Type PropertyType { get; set; } = Type.INVALID;
        public bool IsList { get; set; } = false;
        public Type ListType { get; set; } = Type.INVALID;
        public List<int> ListCounts { get; set; } = new List<int>();
    }

    public static class PlyHelper
    {
        public static Dictionary<Type, PropertyInfo> PropertyTable = new Dictionary<Type, PropertyInfo>
        {
            { Type.INT8,    new PropertyInfo(1, "char") },
            { Type.UINT8,   new PropertyInfo(1, "uchar") },
            { Type.INT16,   new PropertyInfo(2, "short") },
            { Type.UINT16,  new PropertyInfo(2, "ushort") },
            { Type.INT32,   new PropertyInfo(4, "int") },
            { Type.UINT32,  new PropertyInfo(4, "int") },
            { Type.FLOAT32, new PropertyInfo(4, "float") },
            { Type.FLOAT64, new PropertyInfo(8, "double") },
            { Type.INVALID, new PropertyInfo(0, "INVALID") }
        };

        internal static class EndianSwapHelper
        {
            private static void EndianSwapForDefault<T>(byte[] data, int offset)
            {
                return;
            }

            private static void EndianSwapForUInt16(byte[] data, int offset)
            {
                UInt16 v = BitConverter.ToUInt16(data, offset);
                UInt32 vv = v;
                var result = (UInt16)((vv << 8) | (vv >> 8));
                var bytes = BitConverter.GetBytes(result);
                Array.Copy(bytes, 0, data, offset, bytes.Length);
            }

            private static void EndianSwapForUInt32(byte[] data, int offset)
            {
                UInt32 v = BitConverter.ToUInt32(data, offset);
                UInt32 result = (v << 24) | ((v << 8) & 0x00ff0000) | ((v >> 8) & 0x0000ff00) | (v >> 24);
                var bytes = BitConverter.GetBytes(result);
                Array.Copy(bytes, 0, data, offset, bytes.Length);
            }

            private static void EndianSwapForUInt64(byte[] data, int offset)
            {
                UInt64 v = BitConverter.ToUInt64(data, offset);
                UInt64 result = (((v & 0x00000000000000ffL) << 56) |
                        ((v & 0x000000000000ff00L) << 40) |
                        ((v & 0x0000000000ff0000L) << 24) |
                        ((v & 0x00000000ff000000L) << 8) |
                        ((v & 0x000000ff00000000L) >> 8) |
                        ((v & 0x0000ff0000000000L) >> 24) |
                        ((v & 0x00ff000000000000L) >> 40) |
                        ((v & 0xff00000000000000L) >> 56));
                var bytes = BitConverter.GetBytes(result);
                Array.Copy(bytes, 0, data, offset, bytes.Length);
            }

            private static void EndianSwapForInt16(byte[] data, int offset)
            {
                EndianSwapForUInt16(data, offset);
            }

            private static void EndianSwapForInt32(byte[] data, int offset)
            {
                EndianSwapForUInt32(data, offset);
            }

            private static void EndianSwapForInt64(byte[] data, int offset)
            {
                EndianSwapForUInt64(data, offset);
            }

            private static void EndianSwapForFloat(byte[] data, int offset)
            {
                EndianSwapForUInt32(data, offset);
            }

            private static void EndianSwapForDouble(byte[] data, int offset)
            {
                EndianSwapForUInt64(data, offset);
            }

            static EndianSwapHelper()
            {
                Specializer<UInt16>.Fun = EndianSwapForUInt16;
                Specializer<UInt32>.Fun = EndianSwapForUInt32;
                Specializer<UInt64>.Fun = EndianSwapForUInt64;
                Specializer<Int16>.Fun = EndianSwapForInt16;
                Specializer<Int32>.Fun = EndianSwapForInt32;
                Specializer<Int64>.Fun = EndianSwapForInt64;
                Specializer<float>.Fun = EndianSwapForFloat;
                Specializer<Double>.Fun = EndianSwapForDouble;
                Specializer<byte>.Fun = EndianSwapForDefault<byte>;
                Specializer<sbyte>.Fun = EndianSwapForDefault<sbyte>;
            }

            public static class Specializer<T>
            {
                internal static Action<byte[], int> Fun;
                internal static void Call(byte[] data, int offset) => Fun(data, offset);
            }

            public static void EndianSwapBuffer<T>(byte[] data, int numBytes, int stride)
            {
                int offset = 0;
                for (int count = 0; count < numBytes; count += stride)
                {
                    Specializer<T>.Call(data, offset);
                    offset += stride;
                }
            }
        }

        private static UInt32 _fnv1aBase32 = 0x811C9DC5u;
        private static UInt32 _fnv1aPrime32 = 0x01000193u;

        public static UInt32 HashFnv1a(string str)
        {
            UInt32 result = _fnv1aBase32;
            foreach (var c in str)
            {
                result ^= (UInt32)(c);
                result *= _fnv1aPrime32;
            }

            return result;
        }

        public static Type PropertyTypeFromString(string t)
        {
            if (t == "int8" || t == "char") return Type.INT8;
            else if (t == "uint8" || t == "uchar") return Type.UINT8;
            else if (t == "int16" || t == "short") return Type.INT16;
            else if (t == "uint16" || t == "ushort") return Type.UINT16;
            else if (t == "int32" || t == "int") return Type.INT32;
            else if (t == "uint32" || t == "uint") return Type.UINT32;
            else if (t == "float32" || t == "float") return Type.FLOAT32;
            else if (t == "float64" || t == "double") return Type.FLOAT64;
            return Type.INVALID;
        }

        public static String ReadLine(this BinaryReader reader)
        {
            var result = new StringBuilder();
            bool foundEndOfLine = false;
            char ch;
            while (!foundEndOfLine)
            {
                try
                {
                    ch = reader.ReadChar();
                }
                catch (EndOfStreamException )
                {
                    if (result.Length == 0) return null;
                    else break;
                }

                switch (ch)
                {
                    case '\r':
                        if (reader.PeekChar() == '\n') reader.ReadChar();
                        foundEndOfLine = true;
                        break;
                    case '\n':
                        foundEndOfLine = true;
                        break;
                    default:
                        result.Append(ch);
                        break;
                }
            }
            return result.ToString();
        }

        public static string ReadWord(this BinaryReader stream)
        {
            Encoding encoding = Encoding.Default;
            string word = "";
            // read single character at a time building a word 
            // until reaching whitespace or (-1)
            while (stream.Read()
               .With(c =>
               { // with each character . . .
                 // convert read bytes to char
                   var chr = encoding.GetChars(BitConverter.GetBytes(c)).First();

                   if (c == -1 || (Char.IsWhiteSpace(chr) && !string.IsNullOrEmpty(word)))
                       return -1; //signal end of word
                   else if (!Char.IsWhiteSpace(chr))
                       word = word + chr; //append the char to our word

                   return c;
               }) > -1) ;  // end while(stream.Read() if char returned is -1
            return word;
        }

        public static T With<T>(this T obj, Func<T, T> f)
        {
            return f(obj);
        }

        public static int FindElement(string key, List<PlyElement> list)
        {
            for (int i = 0; i < list.Count; ++i)
            {
                if (list[i].Name == key)
                {
                    return i;
                }
            }

            return -1;
        }

        public static int FindProperty(string key, List<PlyProperty> list)
        {
            for (int i = 0; i < list.Count; ++i)
            {
                if (list[i].Name == key)
                {
                    return i;
                }
            }

            return -1;
        }
    }

    public class PlyElement
    {
        public PlyElement(BinaryReader istream)
        {
            Name = istream.ReadWord();
            Size = Convert.ToInt32(istream.ReadWord());
        }

        public PlyElement(string name, int count)
        {
            Name = name;
            Size = count;
        }

        public string Name { get; set; }
        public int Size { get; set; }
        public List<PlyProperty> Properties { get; set; } = new List<PlyProperty>();
    }

    public class PlyFile
    {
        private PlyFileImpl _impl = new PlyFileImpl();

        /*
         * The ply format requires an ascii header. This can be used to determine at
         * runtime which properties or elements exist in the file. Limited validation of the
         * header is performed; it is assumed the header correctly reflects the contents of the
         * payload. This function may throw. Returns true on success, false on failure.
         */
        public bool ParseHeader(MemoryStream istream)
        {
            BinaryReader br = new BinaryReader(istream);
            return _impl.ParseHeader(br);
        }

        /*
         * Execute a read operation. Data must be requested via `request_properties_from_element(...)`
         * prior to calling this function.
         */
        public void Read(MemoryStream istream)
        {
            _impl.Read(istream);
        }

        /*
         * `write` performs no validation and assumes that the data passed into
         * `add_properties_to_element` is well-formed.
         */
        public void Write(FileStream istream, bool isBinary)
        {
            _impl.Write(istream, isBinary);
        }

        /*
         * These functions are valid after a call to `parse_header(...)`. In the case of
         * writing, get_comments() reference may also be used to add new comments to the ply header.
         */
        public List<PlyElement> GetElements()
        {
            return _impl.Elements;
        }

        public List<string> GetInfo()
        {
            return _impl.ObjInfo;
        }
        public List<string> GetComments()
        {
            return _impl.Comments;
        }

        public bool IsBinaryFile()
        {
            return _impl.IsBinary;
        }

        /*
         * In the general case where |list_size_hint| is zero, `read` performs a two-pass
         * parse to support variable length lists. The most general use of the
         * ply format is storing triangle meshes. When this fact is known a-priori, we can pass
         * an expected list length that will apply to this element. Doing so results in an up-front
         * memory allocation and a single-pass import, a 2x performance optimization.
         */
        public PlyData RequestPropertiesFromElement(string elementKey, List<string> propertyKeys, int listSizeHint = 0)
        {
            return _impl.RequestPropertiesFromElement(elementKey, propertyKeys, listSizeHint);
        }

        public void AddPropertiesToElement(string elementKey,
            List<string> propertyKeys,
            Type type,
            int count,
            byte[] data,
            Type listType,
            List<int> listCounts)
        {
            _impl.AddPropertiesToElement(elementKey, propertyKeys, type, count, data, listType, listCounts);
        }

        public void AddPropertiesToElement(string elementKey,
            List<string> propertyKeys,
            Type type,
            int count,
            byte[] data,
            Type listType,
            int listCount)
        {
            _impl.AddPropertiesToElement(elementKey, propertyKeys, type, count, data, listType, listCount);
        }
    }

    public class PlyFileImpl
    {
        public class PlyDataCursor
        {
            public int ByteOffset = 0;
            public int TotalSizeBytes { get; set; } = 0;
        }

        public class ParsingHelper
        {
            public PlyData Data { get; set; }
            public PlyDataCursor Cursor { get; set; }
            public int ListSizeHint { get; set; }

        }

        public class PropertyLookup
        {
            public ParsingHelper Helper { get; set; } = null;
            public bool Skip { get; set; } = false;
            public int PropStride { get; set; } = 0;
            public int ListStride { get; set; } = 0;
        }

        Dictionary<UInt32, ParsingHelper> _userData = new Dictionary<UInt32, ParsingHelper>();

        public bool IsBinary { get; set; } = false;
        public bool IsBigEndian { get; set; } = false;

        private List<PlyElement> _elements = new List<PlyElement>();
        public List<PlyElement> Elements { get => _elements; }

        private List<string> _comments = new List<string>();
        public List<string> Comments { get => _comments; }

        private List<string> _objInfo = new List<string>();
        public List<string> ObjInfo { get => _objInfo; }

        private byte[] _scratch = new byte[64];  // large enough for max list size

        public void Read(MemoryStream istream)
        {
            List<PlyData> buffers = new List<PlyData>();
            foreach (var entry in _userData)
            {
                buffers.Add(entry.Value.Data);
            }

            // Discover if we can allocate up front without parsing the file twice
            int listHints = 0;
            foreach (var b in buffers)
            {
                foreach (var entry in _userData)
                {
                    listHints += entry.Value.ListSizeHint;
                }
            }

            // No list hints? Then we need to calculate how much memory to allocate
            if (listHints == 0)
            {
                ParseData(istream, true);
            }

            // Count the number of properties (required for allocation)
            // e.g. if we have properties x y and z requested, we ensure
            // that their buffer points to the same PlyData
            Dictionary<PlyData, int> uniqueDataCount = new Dictionary<PlyData, int>();
            foreach (var data in buffers)
            {
                if (uniqueDataCount.ContainsKey(data))
                {
                    uniqueDataCount[data] += 1;
                }
                else
                {
                    uniqueDataCount.Add(data, 1);
                }
            }

            // Since group-requested properties share the same cursor,
            // we need to find unique cursors so we only allocate once
            buffers = buffers.Distinct().ToList();

            // We sorted by ptrs on PlyData, need to remap back onto its cursor in the userData table
            foreach (var b in buffers)
            {
                foreach (var entry in _userData)
                {
                    if (entry.Value.Data == b && b.Buffer?.Get() == null)
                    {
                        // If we didn't receive any list hints, it means we did two passes over the
                        // file to compute the total length of all (potentially) variable-length lists
                        if (listHints == 0)
                        {
                            b.Buffer = new Buffer(entry.Value.Cursor.TotalSizeBytes);
                        }
                        else
                        {
                            // otherwise, we can allocate up front, skipping the first pass.
                            int listSizeMultiplier = entry.Value.Data.IsList ? entry.Value.ListSizeHint : 1;
                            var bytesPerProperty = entry.Value.Data.Count * PlyHelper.PropertyTable[entry.Value.Data.T].Stride * listSizeMultiplier;
                            bytesPerProperty *= uniqueDataCount[b];
                            b.Buffer = new Buffer(bytesPerProperty);
                        }

                    }
                }
            }

            // Populate the data
            ParseData(istream, false);

            // In-place big-endian to little-endian swapping if required
            if (IsBigEndian)
            {
                foreach (var b in buffers)
                {
                    byte[] data = b.Buffer.Get();
                    var stride = PlyHelper.PropertyTable[b.T].Stride;
                    var bufferSizeBytes = b.Buffer.SizeBytes();
                
                    switch (b.T)
                    {
                        case Type.INT16:
                            PlyHelper.EndianSwapHelper.EndianSwapBuffer<Int16>(data, (int)bufferSizeBytes, stride);
                            break;
                        case Type.UINT16:
                            PlyHelper.EndianSwapHelper.EndianSwapBuffer<UInt16>(data, (int)bufferSizeBytes, stride);
                            break;
                        case Type.INT32:
                            PlyHelper.EndianSwapHelper.EndianSwapBuffer<Int32>(data, (int)bufferSizeBytes, stride);
                            break;
                        case Type.UINT32:
                            PlyHelper.EndianSwapHelper.EndianSwapBuffer<UInt32>(data, (int)bufferSizeBytes, stride);
                            break;
                        case Type.FLOAT32:
                            PlyHelper.EndianSwapHelper.EndianSwapBuffer<float>(data, (int)bufferSizeBytes, stride);
                            break;
                        case Type.FLOAT64:
                            PlyHelper.EndianSwapHelper.EndianSwapBuffer<double>(data, (int)bufferSizeBytes, stride);
                            break;
                        default:
                            break;
                    }

                }
            }
        }

        public void Write(FileStream ostream, bool isBinary)
        {
            foreach (var d in _userData)
            {
                d.Value.Cursor.ByteOffset = 0;
            }

            if (isBinary)
            {
                IsBinary = true;
                IsBigEndian = false;
                BinaryWriter br = new BinaryWriter(ostream, Encoding.Default);
                WriteBinaryInternal(br);
                br.Flush();
                br.Close();
            }
            else
            {
                IsBinary = false;
                IsBigEndian = false;
                StreamWriter sw = new StreamWriter(ostream, Encoding.Default);
                WriteAsciiInternal(sw);
                sw.Flush();
                sw.Close();
            }
        }

        public PlyData RequestPropertiesFromElement(string elementKey,
            List<string> propertyKeys,
            int listSizeHint)
        {
            if (_elements.Count == 0)
            {
                throw new ArgumentException("header had no elements defined. malformed file?");
            }

            if (string.IsNullOrEmpty(elementKey))
            {
                throw new ArgumentException("`elementKey` argument is empty");
            }

            if (propertyKeys.Count == 0)
            {
                throw new ArgumentException("`propertyKeys` argument is empty");
            }

            PlyData outData = new PlyData();

            int elementIdx = PlyHelper.FindElement(elementKey, _elements);

            List<string> keysNotFound = new List<string>();

            // Sanity check if the user requested element is in the pre-parsed header
            if (elementIdx >= 0)
            {
                // We found the element
                PlyElement element = _elements[elementIdx];

                // Each key in `propertyKey` gets an entry into the userData map (keyed by a hash of
                // element name and property name), but groups of properties (requested from the
                // public api through this function) all share the same `ParsingHelper`. When it comes
                // time to .read(), we check the number of unique PlyData shared pointers
                // and allocate a single buffer that will be used by each property key group.
                // That way, properties like, {"x", "y", "z"} will all be put into the same buffer.

                ParsingHelper helper = new ParsingHelper();
                helper.Data = outData;
                helper.Data.Count = element.Size; // how many items are in the element?
                helper.Data.IsList = false;
                helper.Data.T = Type.INVALID;
                helper.Cursor = new PlyDataCursor();
                helper.ListSizeHint = listSizeHint;

                // Find each of the keys
                foreach (var key in propertyKeys)
                {
                    int propertyIdx = PlyHelper.FindProperty(key, element.Properties);
                    if (propertyIdx < 0) keysNotFound.Add(key);
                }

                if (keysNotFound.Count != 0)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach (var str in keysNotFound) sb.Append(str + ", ");
                    throw new ArgumentException("the following property keys were not found in the header: " + sb.ToString());
                }

                foreach (var key in propertyKeys)
                {
                    int propertyIdx = PlyHelper.FindProperty(key, element.Properties);
                    PlyProperty property = element.Properties[propertyIdx];
                    helper.Data.T = property.PropertyType;
                    helper.Data.IsList = property.IsList;
                    _userData.Add(PlyHelper.HashFnv1a(element.Name + property.Name), helper);
                }

                // Sanity check that all properties share the same type
                List<Type> propertyTypes = new List<Type>();
                foreach (var key in propertyKeys)
                {
                    int propertyIdx = PlyHelper.FindProperty(key, element.Properties);
                    PlyProperty property = element.Properties[propertyIdx];
                    propertyTypes.Add(property.PropertyType);
                }

                var type = propertyTypes[0];
                for (int i = 1; i < propertyTypes.Count; i++)
                {
                    if (type != propertyTypes[i])
                    {
                        throw new ArgumentException("all requested properties must share the same type.");
                    }
                }
            }
            else
            {
                throw new ArgumentException("the element key was not found in the header: " + elementKey);
            }

            return outData;
        }

        public void AddPropertiesToElement(string elementKey,
            List<string> propertyKeys,
            Type type,
            int count,
            byte[] data,
            Type listType,
            List<int> listCounts)
        {
            ParsingHelper helper = new ParsingHelper();
            helper.Data = new PlyData();
            helper.Data.Count = count;
            helper.Data.T = type;
            helper.Data.Buffer = new Buffer(data);
            helper.Cursor = new PlyDataCursor();

            Action<PlyElement> createPropertyOnElement = (PlyElement e) =>
            {
                foreach (var key in propertyKeys)
                {
                    PlyProperty newProp = (listType == Type.INVALID) ? new PlyProperty(type, key) : new PlyProperty(listType, type, key, listCounts);
                    _userData.Add(PlyHelper.HashFnv1a(elementKey + key), helper);
                    e.Properties.Add(newProp);
                }
            };

            int idx = PlyHelper.FindElement(elementKey, _elements);
            if (idx >= 0)
            {
                PlyElement e = _elements[idx];
                createPropertyOnElement(e);
            }
            else
            {
                PlyElement newElement = (listType == Type.INVALID) ? new PlyElement(elementKey, count) : new PlyElement(elementKey, count);
                createPropertyOnElement(newElement);
                _elements.Add(newElement);
            }
        }

        public void AddPropertiesToElement(string elementKey,
            List<string> propertyKeys,
            Type type,
            int count,
            byte[] data,
            Type listType,
            int listCount)
        {
            List<int> listCounts = new List<int>();
            if (listCount != 0)
            {
                for (int i = 0; i < count; ++i)
                {
                    listCounts.Add(listCount);
                }
            }
            AddPropertiesToElement(elementKey, propertyKeys, type, count, data, listType, listCounts);
        }

        int ReadPropertyBinary(int stride, byte[] dest, ref int destOffset, BinaryReader istream)
        {
            var readedBytes = istream.ReadBytes(stride);
            Array.Copy(readedBytes, 0, dest, destOffset, readedBytes.Length);
            destOffset += stride;
            return stride;
        }

        int ReadPropertyAscii(Type t, int stride, byte[] dest, ref int destOffset, BinaryReader istream)
        {
            switch (t)
            {
                case Type.INT8:
                case Type.UINT8:
                    dest[destOffset] = Convert.ToByte(istream.ReadWord());
                    break;
                case Type.INT16:
                    {
                        Int16 data = Convert.ToInt16(istream.ReadWord());
                        var bytes = BitConverter.GetBytes(data);
                        Array.Copy(bytes, 0, dest, destOffset, bytes.Length);
                    }
                    break;
                case Type.UINT16:
                    {
                        UInt16 data = Convert.ToUInt16(istream.ReadWord());
                        var bytes = BitConverter.GetBytes(data);
                        Array.Copy(bytes, 0, dest, destOffset, bytes.Length);
                    }
                    break;
                case Type.INT32:
                    {
                        Int32 data = Convert.ToInt32(istream.ReadWord());
                        var bytes = BitConverter.GetBytes(data);
                        Array.Copy(bytes, 0, dest, destOffset, bytes.Length);
                    }
                    break;
                case Type.UINT32:
                    {
                        UInt32 data = Convert.ToUInt32(istream.ReadWord());
                        var bytes = BitConverter.GetBytes(data);
                        Array.Copy(bytes, 0, dest, destOffset, bytes.Length);
                    }
                    break;
                case Type.FLOAT32:
                    {
                        float data = Convert.ToSingle(istream.ReadWord());
                        var bytes = BitConverter.GetBytes(data);
                        Array.Copy(bytes, 0, dest, destOffset, bytes.Length);
                    }
                    break;
                case Type.FLOAT64:
                    {
                        double data = Convert.ToDouble(istream.ReadWord());
                        var bytes = BitConverter.GetBytes(data);
                        Array.Copy(bytes, 0, dest, destOffset, bytes.Length);
                    }
                    break;
                case Type.INVALID:
                    throw new ArgumentException("invalid ply property");
            }
            destOffset += stride;
            return stride;
        }

        List<List<PropertyLookup>> MakePropertyLookupTable()
        {
            List<List<PropertyLookup>> elementPropertyLookup = new List<List<PropertyLookup>>();

            foreach (var element in _elements)
            {
                List<PropertyLookup> lookups = new List<PropertyLookup>();
                foreach (var property in element.Properties)
                {
                    PropertyLookup f = new PropertyLookup();
                    if (_userData.TryGetValue(PlyHelper.HashFnv1a(element.Name + property.Name), out ParsingHelper helper))
                    {
                        f.Helper = helper;
                    }
                    else
                    {
                        f.Skip = true;
                    }

                    f.PropStride = PlyHelper.PropertyTable[property.PropertyType].Stride;
                    if (property.IsList)
                    {
                        f.ListStride = PlyHelper.PropertyTable[property.ListType].Stride;
                    }

                    lookups.Add(f);
                }

                elementPropertyLookup.Add(lookups);
            }

            return elementPropertyLookup;
        }

        public bool ParseHeader(BinaryReader istream)
        {
            string line;
            bool success = true;
            while ((line = istream.ReadLine()) != null)
            {
                line = line.Trim();
                byte[] array = Encoding.Default.GetBytes(line);
                MemoryStream stream = new MemoryStream(array);
                BinaryReader ls = new BinaryReader(stream);
                string token = ls.ReadWord();
                if (token == "ply" || token == "PLY" || token == "") continue;
                else if (token == "comment") ReadHeaderText(line, ref _comments, 8);
                else if (token == "format") ReadHeaderFormat(ls);
                else if (token == "element") ReadHeaderElement(ls);
                else if (token == "property") ReadHeaderProperty(ls);
                else if (token == "obj_info") ReadHeaderText(line, ref _objInfo, 9);
                else if (token == "end_header") break;
                else success = false;
            }

            return success;
        }

        public delegate void DelegateRead(PropertyLookup f, PlyProperty p, byte[] dest, ref int destOffset, BinaryReader istream);
        public delegate int DelegateSkip(PropertyLookup f, PlyProperty p, BinaryReader istream);
        public delegate int DelegateReadListBinary(Type t, ref int listSize, int stride, BinaryReader istream);
        public delegate int DelegateReadListAscii(Type t, ref int listSize, int stride, BinaryReader istream);
        void ParseData(MemoryStream istream, bool firstPass)
        {
            DelegateRead read;
            DelegateSkip skip;

            var start = istream.Position;
            BinaryReader breader = new BinaryReader(istream);

            int listSize = 0;

            // Special case mirroring read_property_binary but for list types; this
            // has an additional big endian check to flip the data in place immediately
            // after reading. We do this as a performance optimization; endian flipping is
            // done on regular properties as a post-process after reading (also for optimization)
            // but we need the correct little-endian list count as we read the file.
            DelegateReadListBinary readListBinary = (Type t, ref int listSize, int stride, BinaryReader istream) =>
            {
                var bytes = istream.ReadBytes(stride);

                switch (t)
                {
                    case Type.INT16:
                        if (IsBigEndian) PlyHelper.EndianSwapHelper.EndianSwapBuffer<Int16>(bytes, 0, stride);
                        listSize = (int)BitConverter.ToInt16(bytes);
                        break;
                    case Type.UINT16:
                        if (IsBigEndian) PlyHelper.EndianSwapHelper.EndianSwapBuffer<UInt16>(bytes, 0, stride);
                        listSize = (int)BitConverter.ToUInt16(bytes);
                        break;
                    case Type.INT32:
                        if (IsBigEndian) PlyHelper.EndianSwapHelper.EndianSwapBuffer<Int32>(bytes, 0, stride);
                        listSize = (int)BitConverter.ToInt32(bytes);
                        break;
                    case Type.UINT32:
                        if (IsBigEndian) PlyHelper.EndianSwapHelper.EndianSwapBuffer<UInt32>(bytes, 0, stride);
                        listSize = (int)BitConverter.ToUInt32(bytes);
                        break;
                    case Type.UINT8:
                    case Type.INT8:
                        listSize = (int)bytes[0];
                        break;
                    default:
                        break;
                }

                return stride;
            };

            DelegateReadListAscii readListAscii = (Type t, ref int listSize, int stride, BinaryReader istream) =>
            {
                switch (t)
                {
                    case Type.UINT8:
                    case Type.INT8:
                        listSize = Convert.ToByte(istream.ReadWord());
                        break;
                    case Type.INT16:
                        listSize = Convert.ToInt16(istream.ReadWord());
                        break;
                    case Type.UINT16:
                        listSize = Convert.ToUInt16(istream.ReadWord());
                        break;
                    case Type.INT32:
                        listSize = Convert.ToInt32(istream.ReadWord());
                        break;
                    case Type.UINT32:
                        listSize = (int)Convert.ToUInt32(istream.ReadWord());
                        break;
                    default:
                        break;
                }

                return stride;
            };


            if (IsBinary)
            {
                read = (PropertyLookup f, PlyProperty p, byte[] dest, ref int destOffset, BinaryReader istream) =>
                {
                    if (!p.IsList)
                    {
                        ReadPropertyBinary(f.PropStride, dest, ref destOffset, istream); 
                    }
                    else
                    {
                        readListBinary(p.ListType, ref listSize, f.ListStride, istream);
                        ReadPropertyBinary(f.PropStride * listSize, dest, ref destOffset, istream);
                    }
                };

                skip = (PropertyLookup f, PlyProperty p, BinaryReader istream) =>
                {
                    if (!p.IsList)
                    {
                        var bytes = istream.ReadBytes(f.PropStride);
                        Array.Copy(bytes, 0, _scratch, 0, f.PropStride);
                        return f.PropStride;
                    }

                    readListBinary(p.ListType, ref listSize, f.ListStride, istream);
                    var bytesToSkip = f.PropStride * listSize;
                    istream.ReadBytes(bytesToSkip);
                    return bytesToSkip;
                };
            }
            else
            {
                read = (PropertyLookup f, PlyProperty p, byte[] dest, ref int destOffset, BinaryReader istream) =>
                {
                    if (!p.IsList)
                    {
                        ReadPropertyAscii(p.PropertyType, f.PropStride, dest, ref destOffset, istream);
                    }
                    else
                    {
                        readListAscii(p.ListType, ref listSize, f.ListStride, istream);
                        for (int i = 0; i < listSize; ++i)
                        {
                            ReadPropertyAscii(p.PropertyType, f.PropStride, dest, ref destOffset, istream);
                        }
                    }
                };

                skip = (PropertyLookup f, PlyProperty p, BinaryReader istream) =>
                {
                    if (p.IsList)
                    {
                        readListAscii(p.ListType, ref listSize, f.ListStride, istream);
                        for (int i = 0; i < listSize; ++i)
                        {
                            var word2 = istream.ReadWord();
                        }
                        return listSize * f.PropStride;
                    }

                    var word = istream.ReadWord();
                    return f.PropStride;
                };
            }

            var elementPropertyLookup = MakePropertyLookupTable();
            int elementIdx = 0;
            int propertyIdx = 0;
            ParsingHelper helper;

            // This is the inner import loop
            foreach (var element in _elements)
            {
                for (int count = 0; count < element.Size; ++count)
                {
                    propertyIdx = 0;
                    foreach (var property in element.Properties)
                    {
                        PropertyLookup lookup = elementPropertyLookup[elementIdx][propertyIdx];

                        if (!lookup.Skip)
                        {
                            helper = lookup.Helper;
                            if (firstPass)
                            {
                                helper.Cursor.TotalSizeBytes += skip(lookup, property, breader);

                                if (property.IsList)
                                {
                                    property.ListCounts.Add(listSize);
                                }
                            }
                            else
                            {
                                read(lookup, property, helper.Data.Buffer.Get(), ref helper.Cursor.ByteOffset, breader);
                            }
                        }
                        else
                        {
                            skip(lookup, property, breader);
                        }
                        propertyIdx++;
                    }
                }
                elementIdx++;
            }

            // Reset istream position to the start of the data
            if (firstPass) istream.Position = start;
        }

        void ReadHeaderFormat(BinaryReader istream)
        {
            string s = istream.ReadWord();
            if (s == "binary_little_endian") IsBinary = true;
            else if (s == "binary_big_endian") IsBinary = IsBigEndian = true;
        }

        void ReadHeaderElement(BinaryReader istream)
        {
            _elements.Add(new PlyElement(istream));
        }

        void ReadHeaderProperty(BinaryReader istream)
        {
            if (_elements.Count == 0) throw new ApplicationException("no elements defined; file is malformed");
            _elements.Last().Properties.Add(new PlyProperty(istream));
        }

        void ReadHeaderText(string line, ref List<string> place, int erase = 0)
        {
            place.Add((erase > 0) ? line.Substring(erase) : line);
        }

        void WriteHeader(BinaryWriter ostream)
        {
            ostream.Write("ply\n".ToCharArray());
            if (IsBinary)
            {
                ostream.Write((IsBigEndian) ? "format binary_big_endian 1.0\n".ToCharArray() : "format binary_little_endian 1.0\n".ToCharArray());
            }
            else
            {
                ostream.Write("format ascii 1.0\n".ToCharArray());
            }

            foreach (var comment in _comments)
            {
                ostream.Write("comment ".ToCharArray());
                ostream.Write(comment.ToCharArray());
                ostream.Write("\n".ToCharArray());
            }

            var propertyLookup = MakePropertyLookupTable();
            int elementIdx = 0;
            foreach (var e in _elements)
            {
                ostream.Write(string.Format("element {0} {1}\n", e.Name, e.Size).ToCharArray());
                int propertyIdx = 0;
                foreach (var p in e.Properties)
                {
                    PropertyLookup lookup = propertyLookup[elementIdx][propertyIdx];
                    if (!lookup.Skip)
                    {
                        if (p.IsList)
                        {
                            ostream.Write(string.Format("property list {0} {1} {2}\n",
                                PlyHelper.PropertyTable[p.ListType].Str,
                                PlyHelper.PropertyTable[p.PropertyType].Str, p.Name).ToCharArray());
                        }
                        else
                        {
                            ostream.Write(string.Format("property {0} {1}\n",
                                PlyHelper.PropertyTable[p.PropertyType].Str, p.Name).ToCharArray());
                        }
                    }
                    propertyIdx++;
                }
                elementIdx++;
            }

            ostream.Write("end_header\n".ToCharArray());
        }

        void WriteHeader(StreamWriter ostream)
        {
            ostream.Write("ply\n");
            if (IsBinary)
            {
                ostream.Write((IsBigEndian) ? "format binary_big_endian 1.0\n" : "format binary_little_endian 1.0\n");
            }
            else
            {
                ostream.Write("format ascii 1.0\n");
            }

            foreach (var comment in _comments)
            {
                ostream.Write("comment ");
                ostream.Write(comment);
                ostream.Write("\n");
            }

            var propertyLookup = MakePropertyLookupTable();
            int elementIdx = 0;
            foreach (var e in _elements)
            {
                ostream.Write(string.Format("element {0} {1}\n", e.Name, e.Size));
                int propertyIdx = 0;
                foreach (var p in e.Properties)
                {
                    PropertyLookup lookup = propertyLookup[elementIdx][propertyIdx];
                    if (!lookup.Skip)
                    {
                        if (p.IsList)
                        {
                            ostream.Write(string.Format("property list {0} {1} {2}\n",
                                PlyHelper.PropertyTable[p.ListType].Str,
                                PlyHelper.PropertyTable[p.PropertyType].Str, p.Name));
                        }
                        else
                        {
                            ostream.Write(string.Format("property {0} {1}\n",
                                PlyHelper.PropertyTable[p.PropertyType].Str, p.Name));
                        }
                    }
                    propertyIdx++;
                }
                elementIdx++;
            }

            ostream.Write("end_header\n");
        }


        void WriteAsciiInternal(StreamWriter ostream)
        {
            WriteHeader(ostream);

            var elementPropertyLookup = MakePropertyLookupTable();

            int elementIdx = 0;
            foreach (var e in _elements)
            {
                for (int i = 0; i < e.Size; ++i)
                {
                    int propertyIdx = 0;
                    foreach (var p in e.Properties)
                    {
                        var f = elementPropertyLookup[elementIdx][propertyIdx];
                        var helper = f.Helper;
                        if (f.Skip || helper == null) continue;

                        if (p.IsList)
                        {
                            ostream.Write(p.ListCounts[i]);
                            ostream.Write(' ');
                            for (int j = 0; j < p.ListCounts[i]; ++j)
                            {
                                WritePropertyAscii(p.PropertyType, ostream, helper.Data.Buffer.Get(), ref helper.Cursor.ByteOffset);
                            }
                        }
                        else
                        {
                            WritePropertyAscii(p.PropertyType, ostream, helper.Data.Buffer.Get(), ref helper.Cursor.ByteOffset);
                        }
                        propertyIdx++;
                    }
                    ostream.Write('\n');
                }
                elementIdx++;
            }
        }

        void WriteBinaryInternal(BinaryWriter ostream)
        {
            IsBinary = true;
            WriteHeader(ostream);

            byte[] listSize = { 0, 0, 0, 0 };
            int dummyCount = 0;

            var elementPropertyLookup = MakePropertyLookupTable();

            int elementIdx = 0;
            foreach (var e in _elements)
            {
                for (int i = 0; i < e.Size; ++i)
                {
                    int propertyIdx = 0;
                    foreach (var p in e.Properties)
                    {
                        var f = elementPropertyLookup[elementIdx][propertyIdx];
                        var helper = f.Helper;
                        if (f.Skip || helper == null) continue;

                        if (p.IsList)
                        {
                            var bytes = BitConverter.GetBytes(p.ListCounts[i]);
                            dummyCount = 0;
                            WritePropertyBinary(ostream, bytes, ref dummyCount, f.ListStride);
                            WritePropertyBinary(ostream, helper.Data.Buffer.Get(), ref helper.Cursor.ByteOffset, f.PropStride * p.ListCounts[i]);
                        }
                        else
                        {
                            WritePropertyBinary(ostream, helper.Data.Buffer.Get(), ref helper.Cursor.ByteOffset, f.PropStride);
                        }
                        propertyIdx++;
                    }
                }
                elementIdx++;
            }
        }

        void WritePropertyAscii(Type t, StreamWriter ostream, byte[] src, ref int srcOffset)
        {
            switch (t)
            {
                case Type.INT8:
                case Type.UINT8:
                    ostream.Write(src[srcOffset]);
                    break;
                case Type.INT16:
                    {
                        Int16 data = BitConverter.ToInt16(src, srcOffset);
                        ostream.Write(data);
                    }
                    break;
                case Type.UINT16:
                    {
                        UInt16 data = BitConverter.ToUInt16(src, srcOffset);
                        ostream.Write(data);
                    }
                    break;
                case Type.INT32:
                    {
                        Int32 data = BitConverter.ToInt32(src, srcOffset);
                        ostream.Write(data);
                    }
                    break;
                case Type.UINT32:
                    {
                        UInt32 data = BitConverter.ToUInt32(src, srcOffset);
                        ostream.Write(data);
                    }
                    break;
                case Type.FLOAT32:
                    {
                        float data = BitConverter.ToSingle(src, srcOffset);
                        ostream.Write(data);
                    }
                    break;
                case Type.FLOAT64:
                    {
                        double data = BitConverter.ToDouble(src, srcOffset);
                        ostream.Write(data);
                    }
                    break;
                case Type.INVALID:
                    throw new ArgumentException("invalid ply property");
            }
            ostream.Write(' ');
            srcOffset += PlyHelper.PropertyTable[t].Stride;
        }

        void WritePropertyBinary(BinaryWriter ostream, byte[] src, ref int srcOffset,  int stride)
        {
            byte[] byteForWrite = new byte[stride];
            Array.Copy(src, srcOffset, byteForWrite, 0, stride);
            ostream.Write(byteForWrite);
            srcOffset += stride;
        }
    }

}