using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

namespace DS
{
    public class file_ops
    {
        [SqlFunction(FillRowMethodName = "ls_fill_row")]
        public static IEnumerable ls(SqlString Path, SqlString Pattern)
        {
            return
                new List<string>
                    (
                        Directory.GetFiles((string)Path, (string)Pattern, SearchOption.TopDirectoryOnly)
                    );
        }

        public static void ls_fill_row(
            object obj,
            out SqlString Name, out SqlString Extension,
            out SqlInt64 SizeBytes,
            out SqlDateTime CreationTime, out SqlDateTime CreationTimeUtc,
            out SqlDateTime LastWriteTime, out SqlDateTime LastWriteTimeUtc)
        {
            FileInfo FI = new FileInfo((string)obj);

            Name = FI.Name;
            Extension = FI.Extension;

            SizeBytes = FI.Length;

            CreationTime = FI.CreationTime;
            CreationTimeUtc = FI.CreationTimeUtc;

            LastWriteTime = FI.LastWriteTime;
            LastWriteTimeUtc = FI.LastWriteTimeUtc;
        }

        [SqlProcedure]
        public static void mv(SqlString SourcePathName, SqlString DestPathName)
        {
            File.Move((string)SourcePathName, (string)DestPathName);
        }

        [SqlProcedure]
        public static void ren(SqlString Path, SqlString OldName, SqlString NewName)
        {
            mv(Path + @"\" + OldName, Path + @"\" + NewName);
        }

        [SqlProcedure]
        public static void rm(SqlString PathName)
        {
            if (File.Exists((string)PathName))
                File.Delete((string)PathName);
            else
                throw new FileNotFoundException(string.Format("File \"{0}\" does not exist.", PathName));
        }
    }
}
