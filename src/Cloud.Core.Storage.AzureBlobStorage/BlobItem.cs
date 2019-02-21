namespace Cloud.Core.Storage.AzureBlobStorage
{
    using System;
    using System.Globalization;
    using JetBrains.Annotations;
    using Microsoft.WindowsAzure.Storage.Blob;

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

            var index = Path.LastIndexOf("/", StringComparison.InvariantCulture) + 1;
            FileName = Path.Substring(index, Path.Length - index);
            FileNameWithoutExtension = FileName;

            index = Path.LastIndexOf(".", StringComparison.InvariantCulture) + 1;

            if (index > 0)
            {
                FileExtension = Path.Substring(index, Path.Length - index).ToLower(CultureInfo.InvariantCulture);
                FileNameWithoutExtension = FileName.Replace(FileExtension, string.Empty);

                var extIndex = FileNameWithoutExtension.LastIndexOf(".", StringComparison.InvariantCulture);
                FileNameWithoutExtension = extIndex > 0 ? FileNameWithoutExtension.Substring(0, extIndex) : FileNameWithoutExtension;
            }

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobItem"/> class.
        /// </summary>
        /// <param name="item">The <see cref="CloudBlockBlob"/> to build properties from.</param>
        public BlobItem([NotNull] CloudBlockBlob item)
        {
            Tag = item;

            FileName = item.Name;
            FileNameWithoutExtension = item.Name;
            RootFolder = item.Container.Name;
            Path = item.Name;
            UniqueLeaseName = Guid.NewGuid().ToString();

            if (item.Properties != null && item.Properties.Length != -1)
            {
                FileSize = item.Properties.Length;
                ContentHash = item.Properties.ContentMD5;
            }

            var index = Path.LastIndexOf(".", StringComparison.InvariantCulture) + 1;

            if (index > 0) { 
                FileExtension = Path.Substring(index, Path.Length - index).ToLower(CultureInfo.InvariantCulture);
                FileNameWithoutExtension = FileName.Replace(FileExtension, string.Empty);
            }
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
    }
}
