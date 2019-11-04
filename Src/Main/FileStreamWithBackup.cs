using System;
using System.IO;
using System.Text;

namespace USC.GISResearchLab.Common.IOs.FileStreams
{

    // adapted from http://www.codeproject.com/KB/dotnet/customnettracelisteners.aspx

    public class FileStreamWithBackup : FileStream
    {
        #region Properties

        private long _MaxFileLength;
        private int _MaxFileCount;
        private string _FileDir;
        private string _FileBase;
        private string _FileExt;
        private int _FileDecimals;
        private bool _CanSplitData;
        private int _NextFileIndex;

        public long MaxFileLength
        {
            get { return _MaxFileLength; }
            set { _MaxFileLength = value; }
        }
        public int MaxFileCount
        {
            get { return _MaxFileCount; }
            set { _MaxFileCount = value; }
        }
        public bool CanSplitData
        {
            get { return _CanSplitData; }
            set { _CanSplitData = value; }
        }

        public string FileDir
        {
            get { return _FileDir; }
            set { _FileDir = value; }
        }
        public string FileBase
        {
            get { return _FileBase; }
            set { _FileBase = value; }
        }
        public string FileExt
        {
            get { return _FileExt; }
            set { _FileExt = value; }
        }
        public int FileDecimals
        {
            get { return _FileDecimals; }
            set { _FileDecimals = value; }
        }

        public int NextFileIndex
        {
            get { return _NextFileIndex; }
            set { _NextFileIndex = value; }
        }

        #endregion

        public FileStreamWithBackup(string path, long maxFileLength, int maxFileCount, FileMode mode)
            : base(path, BaseFileMode(mode), FileAccess.Write)
        {
            Init(path, maxFileLength, maxFileCount, mode);
        }

        public FileStreamWithBackup(string path, long maxFileLength, int maxFileCount, FileMode mode, FileShare share)
            : base(path, BaseFileMode(mode), FileAccess.Write, share)
        {
            Init(path, maxFileLength, maxFileCount, mode);
        }

        public FileStreamWithBackup(string path, long maxFileLength, int maxFileCount, FileMode mode, FileShare share, int bufferSize)
            : base(path, BaseFileMode(mode), FileAccess.Write, share, bufferSize)
        {
            Init(path, maxFileLength, maxFileCount, mode);
        }

        public FileStreamWithBackup(string path, long maxFileLength, int maxFileCount, FileMode mode, FileShare share, int bufferSize, bool isAsync)
            : base(path, BaseFileMode(mode), FileAccess.Write, share, bufferSize, isAsync)
        {
            Init(path, maxFileLength, maxFileCount, mode);
        }

        public override bool CanRead { get { return false; } }

        public override void Write(byte[] array, int offset, int count)
        {
            int actualCount = System.Math.Min(count, array.GetLength(0));
            if (Position + actualCount <= _MaxFileLength)
            {
                base.Write(array, offset, count);
            }
            else
            {
                if (CanSplitData)
                {
                    int partialCount = (int)(System.Math.Max(_MaxFileLength, Position) - Position);
                    base.Write(array, offset, partialCount);
                    offset += partialCount;
                    count = actualCount - partialCount;
                }
                else
                {
                    if (count > _MaxFileLength)
                        throw new ArgumentOutOfRangeException("Buffer size exceeds maximum file length");
                }
                BackupAndResetStream();
                Write(array, offset, count);
            }
        }

        private void Init(string path, long maxFileLength, int maxFileCount, FileMode mode)
        {
            if (maxFileLength <= 0)
                throw new ArgumentOutOfRangeException("Invalid maximum file length");
            if (maxFileCount <= 0)
                throw new ArgumentOutOfRangeException("Invalid maximum file count");

            MaxFileLength = maxFileLength;
            MaxFileCount = maxFileCount;
            CanSplitData = true;

            string fullPath = Path.GetFullPath(path);
            FileDir = Path.GetDirectoryName(fullPath);
            FileBase = Path.GetFileNameWithoutExtension(fullPath);
            FileExt = Path.GetExtension(fullPath);

            FileDecimals = 1;
            int decimalBase = 10;
            while (decimalBase < _MaxFileCount)
            {
                ++FileDecimals;
                decimalBase *= 10;
            }

            switch (mode)
            {
                case FileMode.Create:
                case FileMode.CreateNew:
                case FileMode.Truncate:
                    // Delete old files
                    for (int iFile = 0; iFile < MaxFileCount; ++iFile)
                    {
                        string file = GetBackupFileName(iFile);
                        if (File.Exists(file))
                            File.Delete(file);
                    }
                    break;

                default:
                    // Position file pointer to the last backup file
                    for (int iFile = 0; iFile < MaxFileCount; ++iFile)
                    {
                        if (File.Exists(GetBackupFileName(iFile)))
                            NextFileIndex = iFile + 1;
                    }
                    if (NextFileIndex == MaxFileCount)
                        NextFileIndex = 0;
                    Seek(0, SeekOrigin.End);
                    break;
            }
        }

        private void BackupAndResetStream()
        {
            Flush();
            File.Copy(Name, GetBackupFileName(NextFileIndex), true);
            SetLength(0);

            ++_NextFileIndex;
            if (NextFileIndex >= MaxFileCount)
                NextFileIndex = 0;
        }

        private string GetBackupFileName(int index)
        {
            StringBuilder format = new StringBuilder();
            format.AppendFormat("D{0}", FileDecimals);
            StringBuilder sb = new StringBuilder();
            if (_FileExt.Length > 0)
                sb.AppendFormat("{0}{1}{2}", FileBase, index.ToString(format.ToString()), FileExt);
            else
                sb.AppendFormat("{0}{1}", FileBase, index.ToString(format.ToString()));
            return Path.Combine(FileDir, sb.ToString());
        }

        private static FileMode BaseFileMode(FileMode mode)
        {
            return mode == FileMode.Append ? FileMode.OpenOrCreate : mode;
        }
    }
}
