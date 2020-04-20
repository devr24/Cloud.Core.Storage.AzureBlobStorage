namespace Cloud.Core.Storage.AzureBlobStorage
{
    using JetBrains.Annotations;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Web;
    using Microsoft.Azure.Storage.Blob;

    /// <summary>
    /// Azure specific implementation of a BLOB item.
    /// </summary>
    /// <seealso cref="Cloud.Core.IBlobItem" />
    public class BlobItem : IBlobItem
    {
        internal BlobItem() { } // for testing

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobItem"/> class.
        /// </summary>
        /// <param name="item">The <see cref="IListBlobItem"/> to build properties from.</param>
        public BlobItem([NotNull] IListBlobItem item)
        {
            Tag = item;

            RootFolder = item.Container.Name;
            Path = item.Uri.AbsolutePath;
            UniqueLeaseName = Guid.NewGuid().ToString();

            var index = Path.LastIndexOf("/", StringComparison.InvariantCulture);
            FileName = Path.Substring(index + 1, Path.Length - index - 1);
            FileNameWithoutExtension = FileName;

            index = Path.LastIndexOf(".", StringComparison.InvariantCulture) + 1;

            if (index > 0)
            {
                FileExtension = Path.Substring(index, Path.Length - index).ToLower(CultureInfo.InvariantCulture);
                FileNameWithoutExtension = FileName.Replace(FileExtension, string.Empty);

                var extIndex = FileNameWithoutExtension.LastIndexOf(".", StringComparison.InvariantCulture);
                FileNameWithoutExtension = extIndex > 0 ? FileNameWithoutExtension.Substring(0, extIndex) : FileNameWithoutExtension;
            }
            Path = $"{RootFolder}/{FileName}";
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobItem"/> class.
        /// </summary>
        /// <param name="item">The <see cref="CloudBlockBlob"/> to build properties from.</param>
        public BlobItem([NotNull] CloudBlockBlob item)
        {
            Tag = item;

            RootFolder = item.Container.Name;
            Path = item.Uri.AbsolutePath;
            UniqueLeaseName = Guid.NewGuid().ToString();

            var index = Path.LastIndexOf("/", StringComparison.InvariantCulture);
            FileName = Path.Substring(index + 1, Path.Length - index - 1);
            FileNameWithoutExtension = FileName;

            index = Path.LastIndexOf(".", StringComparison.InvariantCulture) + 1;

            if (index > 0)
            {
                FileExtension = Path.Substring(index, Path.Length - index).ToLower(CultureInfo.InvariantCulture);
                FileNameWithoutExtension = FileName.Replace(FileExtension, string.Empty);

                var extIndex = FileNameWithoutExtension.LastIndexOf(".", StringComparison.InvariantCulture);
                FileNameWithoutExtension = extIndex > 0 ? FileNameWithoutExtension.Substring(0, extIndex) : FileNameWithoutExtension;
            }

            Metadata = (Dictionary<string, string>) item.Metadata;

            if (item.Properties != null && item.Properties.Length != -1)
            {
                FileSize = item.Properties.Length;

                // Set content hash if it exists.
                if (item.Properties.ContentMD5 != null)
                    ContentHash = ConvertContentHash(item.Properties.ContentMD5);

                // Set LastWriteTime if it exists.
                if (item.Properties.LastModified != null)
                    LastWriteTime = item.Properties.LastModified.Value.UtcDateTime;
            }

            // We need to get the LastWriteTime for the blob, a custom metadata property.
            // We are including a fallback to the built in last modified date of the blob should the custom property not exist.
            // Instantiating the property to DateTime.MinValue just in case something really weird has happened and the fallback property isn't set.
            if (item.Metadata.ContainsKey("LastWriteTimeUtc"))
            {
                var encodedLastWrite = item.Metadata["LastWriteTimeUtc"];
                LastWriteTime = DateTime.Parse(HttpUtility.UrlDecode(encodedLastWrite));
            }
            Path = $"{RootFolder}/{FileName}";
        }

        /// <summary>
        /// Converts Azure's content Md5 property to the safe content hash property 
        /// </summary>
        /// <param name="contentMd5"></param>
        /// <returns></returns>
        internal string ConvertContentHash(string contentMd5)
        {
            var decodedContentHash = Convert.FromBase64String(contentMd5);
            return BitConverter.ToString(decodedContentHash).Replace("-", string.Empty).ToUpper();
        }

        /// <summary>
        /// Gets the name of the file.
        /// </summary>
        /// <value>
        /// The name of the file.
        /// </value>
        public string FileName { get; internal set; }

        /// <summary>Gets the file name without extension.</summary>
        /// <value>The file name without extension.</value>
        public string FileNameWithoutExtension { get; internal set; }

        /// <summary>
        /// Gets the extension of the file.
        /// </summary>
        /// <value>
        /// The file extension, for example ".txt" or ".csv".
        /// </value>
        public string FileExtension { get; internal set; }

        /// <summary>
        /// Gets the tag item (original item this object was generated from).
        /// </summary>
        /// <value>
        /// The tag object.
        /// </value>
        public object Tag { get; internal set; }

        /// <summary>Gets the size of the file.</summary>
        /// <value>The size of the file.</value>
        public long FileSize { get; internal set; }

        /// <summary>
        /// Gets the root folder where this item is contained.
        /// </summary>
        /// <value>
        /// The root folder name.
        /// </value>
        public string RootFolder { get; internal set; }

        /// <summary>
        /// Gets the path to the BLOB item.
        /// </summary>
        /// <value>
        /// The path to the item.
        /// </value>
        public string Path { get; internal set; }

        /// <summary>
        /// Gets the unique name of the lease.
        /// </summary>
        /// <value>
        /// The unique name of the lease.
        /// </value>
        public string UniqueLeaseName { get; set; }

        /// <summary>MD5 has representing blob content</summary>
        public string ContentHash { get; internal set; }

        /// <summary>Last write time of the file.</summary>
        public DateTime LastWriteTime { get; } = DateTime.MinValue;

        /// <summary>
        /// Gets the properties of the blob item.
        /// </summary>
        /// <value>The properties to set for the blob item.</value>
        public Dictionary<string, string> Properties { get; set; }
        
        /// <summary>
        /// Gets the custom metadata of the blob item
        /// </summary>
        public Dictionary<string, string> Metadata { get; set; }
    }
}
