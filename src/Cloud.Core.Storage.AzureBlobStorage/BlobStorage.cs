namespace Cloud.Core.Storage.AzureBlobStorage
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reactive.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Diagnostics.CodeAnalysis;
    using Comparer;
    using Config;
    using Extensions;
    using Microsoft.Azure.Management.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
    using Microsoft.Azure.Services.AppAuthentication;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement;
    using Microsoft.Azure.Storage.RetryPolicies;
    using Microsoft.Extensions.Logging;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Microsoft.Rest;
    using Microsoft.Rest.TransientFaultHandling;
    using Models;

    /// <summary>
    /// Azure specific implementation of BLOB cloud storage.
    /// </summary>
    /// <seealso cref="IBlobStorage" />
    /// <seealso cref="BlobStorageBase" />
    public class BlobStorage : BlobStorageBase, IBlobStorage
    {
        private bool _disposed;
        internal readonly IDictionary<IBlobItem, Timer> LockTimers = new ConcurrentDictionary<IBlobItem, Timer>(ObjectReferenceEqualityComparer<IBlobItem>.Default);

        /// <summary>
        /// Initializes a new instance of <see cref="BlobStorage" /> with Service Principle authentication.
        /// </summary>
        /// <param name="config">The Service Principle configuration settings for connecting to storage.</param>
        /// <param name="logger">The logger to log information to.</param>
        /// <inheritdoc />
        public BlobStorage([NotNull]ServicePrincipleConfig config, [MaybeNull] ILogger logger = null)
            : base(config, logger)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobStorage" /> class with a Connection String.
        /// </summary>
        /// <param name="config">The Connection String information for connecting to Storage.</param>
        /// <param name="logger">The logger.</param>
        public BlobStorage([NotNull]ConnectionConfig config, [MaybeNull] ILogger logger = null)
            : base(config, logger) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobStorage" /> class with Managed Service Identity (MSI) authentication.
        /// </summary>
        /// <param name="config">The Managed Service Identity (MSI) configuration for connecting to storage.</param>
        /// <param name="logger">The logger.</param>
        public BlobStorage([NotNull]MsiConfig config, [MaybeNull]ILogger logger = null)
            : base(config, logger) { }

        /// <summary>
        /// Lists all root level containers.
        /// </summary>
        /// <returns>List of folders found.</returns>
        public async Task<IEnumerable<string>> ListFolders()
        {
            return await ListFolders(false);
        }

        /// <summary>
        /// Lists all root level containers.
        /// </summary>
        /// <param name="fetchBlobAttributes">Fetch extra attributes on the container</param>
        /// <returns>List of folders found.</returns>
        public async Task<IEnumerable<string>> ListFolders(bool fetchBlobAttributes)
        {
            BlobContinuationToken continuationToken = null;
            var results = new List<string>();
            do
            {
                var response = await CloudBlobClient.ListContainersSegmentedAsync(null,
                    fetchBlobAttributes ? ContainerListingDetails.Metadata : ContainerListingDetails.None,
                    null, continuationToken, new BlobRequestOptions(), null, CancellationToken.None);

                continuationToken = response.ContinuationToken;
                results.AddRange(response.Results.Select(i => i.Name));
            }
            while (continuationToken != null);
            return results;
        }

        /// <summary>
        ///  Get a signed access url using supplied permissions and expiry
        /// </summary>
        /// <param name="folderPath">Folder Path we want access to</param>
        /// <param name="signedAccessConfig">Config object with required access permissions and expiry</param>
        /// <returns></returns>
        public async Task<string> GetSignedFolderAccessUrl(string folderPath, ISignedAccessConfig signedAccessConfig)
        {
            var blobPolicyPermissions = GetAzureBlobPolicyPermissions(signedAccessConfig.AccessPermissions);

            var accessPolicy = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = signedAccessConfig.AccessExpiry,
                Permissions = blobPolicyPermissions
            };
            var container = await GetContainer(folderPath);
            var subPath = GetPathWithoutContainer(folderPath);
            var containerSignature = container.GetSharedAccessSignature(accessPolicy);

            string uri;

            if (!string.IsNullOrWhiteSpace(subPath))
            {
                var folder = container.GetDirectoryReference(folderPath.Replace($"{container.Name}/", string.Empty));
                uri = folder.Uri.ToString();
            }
            else
            {
                uri = container.Uri.ToString();
            }

            return $"{uri}{containerSignature}";
        }

        /// <summary>
        /// Get a signed access url with an absolute expiry.
        /// </summary>
        /// <param name="blobPath">Blob to give access to.</param>
        /// <param name="signedAccessConfig">Access config including required permissions and expiry</param>
        /// <returns>String access url.</returns>
        public async Task<string> GetSignedBlobAccessUrl(string blobPath, ISignedAccessConfig signedAccessConfig)
        {
            var blobPolicyPermissions = GetAzureBlobPolicyPermissions(signedAccessConfig.AccessPermissions);
            var blob = await GetBlockBlobReference(blobPath).ConfigureAwait(false);

            var policy = new SharedAccessBlobPolicy
            {
                Permissions = blobPolicyPermissions,
                SharedAccessExpiryTime = signedAccessConfig.AccessExpiry
            };
            return new Uri(blob.Uri, blob.GetSharedAccessSignature(policy)).ToString();
        }

        /// <summary>
        /// Method to translate the generic permissions required to concrete Azure Blob Access Permissions
        /// </summary>
        /// <param name="requiredAccessPermissions">List of permissions to be translated to Azure Blob Permissions</param>
        /// <returns>Enum of Azure Blob Permissions with bitwise operator</returns>
        internal SharedAccessBlobPermissions GetAzureBlobPolicyPermissions(List<AccessPermission> requiredAccessPermissions)
        {
            var azurePermissions = new List<SharedAccessBlobPermissions>();

            foreach (var permission in requiredAccessPermissions)
            {
                switch (permission)
                {
                    case AccessPermission.Add:
                        azurePermissions.Add(SharedAccessBlobPermissions.Add);
                        break;
                    case AccessPermission.Create:
                        azurePermissions.Add(SharedAccessBlobPermissions.Create);
                        break;
                    case AccessPermission.Delete:
                        azurePermissions.Add(SharedAccessBlobPermissions.Delete);
                        break;
                    case AccessPermission.List:
                        azurePermissions.Add(SharedAccessBlobPermissions.List);
                        break;
                    case AccessPermission.None:
                        azurePermissions.Add(SharedAccessBlobPermissions.None);
                        break;
                    case AccessPermission.Read:
                        azurePermissions.Add(SharedAccessBlobPermissions.Read);
                        break;
                    case AccessPermission.Write:
                        azurePermissions.Add(SharedAccessBlobPermissions.Write);
                        break;
                }
            }

            var blobPolicyPermissions = azurePermissions.Aggregate((x, y) => x | y);
            return blobPolicyPermissions;
        }

        /// <summary>
        /// Check if a blob exists using the passed in blob path.
        /// </summary>
        /// <param name="blobPath"></param>
        /// <returns>[True] if exists and [False] if the blob does not exist.</returns>
        public async Task<bool> Exists(string blobPath)
        {
            var cloudBlockBlob = await GetBlockBlobReference(blobPath).ConfigureAwait(false);
            return await cloudBlockBlob.ExistsAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Downloads the BLOB from storage.
        /// </summary>
        /// <param name="blobPath">The BLOB path to download from.</param>
        /// <returns><see cref="Stream"/> containing BLOB content.</returns>
        /// <inheritdoc />
        public async Task<Stream> DownloadBlob(string blobPath)
        {
            Stream memoryStream = new MemoryStream();
            var cloudBlockBlob = await GetBlockBlobReference(blobPath).ConfigureAwait(false);
            await cloudBlockBlob.DownloadToStreamAsync(memoryStream).ConfigureAwait(false);
            memoryStream.Seek(0, SeekOrigin.Begin);
            return memoryStream;
        }

        /// <summary>
        /// Downloads the BLOB from storage.
        /// </summary>
        /// <param name="blobPath">The BLOB path to download from.</param>
        /// <param name="filePath">The file path to download to.</param>
        /// <returns>Async Task</returns>
        /// <inheritdoc />
        public async Task DownloadBlob(string blobPath, string filePath)
        {
            var cloudBlockBlob = await GetBlockBlobReference(blobPath).ConfigureAwait(false);
            await cloudBlockBlob.DownloadToFileAsync(filePath, FileMode.Create).ConfigureAwait(false);
        }

        /// <summary>
        /// Downloads the BLOB from storage.
        /// </summary>
        /// <param name="blob">The BLOB item to download content for.</param>
        /// <returns><see cref="MemoryStream"/> containing BLOB content.</returns>
        public async Task<Stream> DownloadBlob(IBlobItem blob)
        {
            var stream = new MemoryStream();
            await ((CloudBlockBlob)blob.Tag).DownloadToStreamAsync(stream).ConfigureAwait(false);
            stream.Seek(0, SeekOrigin.Begin);
            return stream;
        }

        /// <summary>
        /// Uploads the BLOB to storage.
        /// </summary>
        /// <param name="blobPath">The BLOB path.</param>
        /// <param name="stream">The stream.</param>
        /// <param name="metadata">Properties to add as metadata for the file.</param>
        /// <returns>Async Task</returns>
        /// <inheritdoc />
        public async Task UploadBlob(string blobPath, Stream stream, Dictionary<string, string> metadata = null)
        {
            try
            {
                var cloudBlockBlob = await GetBlockBlobReference(blobPath);
                cloudBlockBlob.Metadata.AddRange(metadata);
                await cloudBlockBlob.UploadFromStreamAsync(stream);
            }
            catch (StorageException ex) when (ex.Message.Contains("container does not exist"))
            {
                Logger?.LogError("Error during upload, cannot find container", ex);
                if (CreateFolderIfNotExists)
                {
                    Logger?.LogInformation("Creating container as it does not exist");
                    await Task.Delay(1000);
                    GetContainer(blobPath, true).GetAwaiter().GetResult();
                    await Task.Delay(1000);
                    UploadBlob(blobPath, stream, metadata).GetAwaiter().GetResult();
                }
                else
                    throw;
            }
        }

        /// <summary>Uploads the BLOB to storage.</summary>
        /// <param name="blobPath">The BLOB path to upload to.</param>
        /// <param name="filePath">The file path to upload from.</param>
        /// <param name="metadata">Properties to add as metadata for the file.</param>
        /// <returns>Async Task</returns>
        /// <inheritdoc />
        public async Task UploadBlob(string blobPath, string filePath, Dictionary<string, string> metadata = null)
        {
            var cloudBlockBlob = await GetBlockBlobReference(blobPath);
            cloudBlockBlob.Metadata.AddRange(metadata);
            await cloudBlockBlob.UploadFromFileAsync(filePath);
        }

        /// <summary>
        /// Adds metadata to the BLOB
        /// </summary>
        /// <param name="blob">The BLOB path.</param>
        /// <returns>Async Task</returns>
        public async Task UpdateBlobMetadata(IBlobItem blob)
        {
            var item = blob as BlobItem;
            if (item == null || item.Tag == null)
                return;

            var cloudBlob = item.Tag as CloudBlockBlob;
            if (cloudBlob == null)
                return;

            await cloudBlob.SetMetadataAsync();
        }

        /// <summary>
        /// Deletes the BLOB from storage.
        /// </summary>
        /// <param name="blobPath">The BLOB path.</param>
        /// <returns>Async Task</returns>
        /// <inheritdoc />
        public async Task DeleteBlob(string blobPath)
        {
            var cloudBlockBlob = await GetBlockBlobReference(blobPath).ConfigureAwait(false);
            await cloudBlockBlob.DeleteIfExistsAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Removes the folder.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns>Task.</returns>
        /// <exception cref="NotImplementedException"></exception>
        public async Task RemoveFolder(string path)
        {
            var container = await GetContainer(path).ConfigureAwait(false);

            var folderPath = path.Replace(container.Name, string.Empty);
            folderPath = ParsePath(folderPath);

            var ctoken = new BlobContinuationToken();
            do
            {
                var result = await container.ListBlobsSegmentedAsync(folderPath, true, BlobListingDetails.None, null, ctoken, null, null).ConfigureAwait(false);
                ctoken = result.ContinuationToken;
                await Task.WhenAll(result.Results
                    .Select(item => (item as CloudBlob)?.DeleteIfExistsAsync())
                    .Where(task => task != null)
                ).ConfigureAwait(false);
            } while (ctoken != null);
        }

        /// <summary>
        /// Adds the folder.
        /// NOTE: We DONT add the sub directories because in Azure Blob Storge, these are virtual and 
        /// don't actually exist until a file has been placed in the directory.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns>Task.</returns>
        /// <exception cref="NotImplementedException"></exception>
        public async Task AddFolder(string path)
        {
            await GetContainer(path, true).ConfigureAwait(false);

            // NOTE: We DONT add the sub directories because in Azure Blob Storge, these are virtual and 
            // don't actually exist until a file has been placed in the directory.
        }

        /// <summary>Gets the BLOB with lock.</summary>
        /// <param name="blobPath">The BLOB path.</param>
        /// <param name="fetchAttributes">if set to <c>true</c> [fetch attributes].</param>
        /// <returns>Task IBlobItem.</returns>
        public async Task<IBlobItem> GetBlob(string blobPath, bool fetchAttributes = false)
        {
            var cloudBlockBlob = await GetBlockBlobReference(blobPath).ConfigureAwait(false);
            var exists = await cloudBlockBlob.ExistsAsync();
            
            // Return null when not found.
            if (!exists)
            {
                return null;
            }

            if (fetchAttributes)
            {
                await cloudBlockBlob.FetchAttributesAsync(new AccessCondition(), new BlobRequestOptions(), new OperationContext()).ConfigureAwait(false);
            }

            return new BlobItem(cloudBlockBlob);
        }

        /// <summary>
        /// Gets the BLOB with lock.
        /// </summary>
        /// <param name="blobPath">The BLOB path.</param>
        /// <param name="leaseName">Name of the lease.</param>
        /// <returns></returns>
        public async Task<IBlobItem> GetBlobWithLock(string blobPath, string leaseName = "")
        {
            var cloudBlockBlob = await GetBlockBlobReference(blobPath).ConfigureAwait(false);

            var exists = await cloudBlockBlob.ExistsAsync();

            // No blob found - return null.
            if (!exists)
            {
                return null;
            }

            var blobItem = new BlobItem(cloudBlockBlob);

            if (!leaseName.IsNullOrEmpty())
                blobItem.UniqueLeaseName = leaseName;

            await cloudBlockBlob.AcquireLeaseAsync(TimeSpan.FromSeconds(ServicePrincipleConfig.LockInSeconds), blobItem.UniqueLeaseName).ConfigureAwait(false);

            LockTimers.Add(
                blobItem,
                new Timer(
                    async _ =>
                    {
                        var accessCondition = new AccessCondition { LeaseId = blobItem.UniqueLeaseName };
                        await cloudBlockBlob.RenewLeaseAsync(accessCondition).ConfigureAwait(false);
                    },
                    null,
                    TimeSpan.FromSeconds(LockTickInSeconds),
                    TimeSpan.FromSeconds(LockTickInSeconds)));

            return blobItem;
        }

        /// <summary>
        /// Unlocks the BLOB item.  This releases the lease on the server.
        /// If this method is not called on any locked item, it will naturally release when the lease expires.
        /// </summary>
        /// <param name="item">The item to release.</param>
        public async void UnlockBlob(IBlobItem item)
        {
            try
            {
                var tag = (CloudBlockBlob)item.Tag;

                var accessCondition = new AccessCondition { LeaseId = item.UniqueLeaseName };
                await tag.ReleaseLeaseAsync(accessCondition).ConfigureAwait(false);
            }
            catch (StorageException)
            {
                // Do nothing if lock did not exist (or file).
            }
            finally
            {
                // Check for a lock renewal timer and release it if it exists.
                if (LockTimers.ContainsKey(item))
                {
                    LockTimers[item]?.Dispose();
                    LockTimers.Remove(item);
                }
            }
        }

        /// <summary>
        /// Gets the files from a requested container and directory.
        /// </summary>
        /// <param name="rootFolder">The path to gather the blob items from.</param>
        /// <param name="recursive">Will traverse sub folders if [recursive] is <c>true</c>.</param>
        /// <param name="fetchBlobAttributes">if set to <c>true</c> [will fetch BLOB attributes]. Warning - this will have a slight performance effect if you switch to true.</param>
        /// <param name="searchPrefix">File name prefix used to filter the search.</param>
        /// <returns><see cref="System.Collections.IEnumerable" /> of <see cref="BlobItem" /> all BLOB items in the path requested.</returns>
        public IEnumerable<IBlobItem> ListBlobs(string rootFolder, bool recursive, bool fetchBlobAttributes = false, string searchPrefix = null)
        {
            return ListBlobsObservable(rootFolder, recursive, fetchBlobAttributes, searchPrefix).ToEnumerable();
        }

        /// <summary>
        /// Lists the blobs observable.
        /// </summary>
        /// <param name="rootFolder">The path to gather the blob items from.</param>
        /// <param name="recursive">Will traverse sub folders if [recursive] is <c>true</c>.</param>
        /// <param name="fetchBlobAttributes">if set to <c>true</c> [will fetch BLOB attributes]. Warning - this will have a slight performance effect if you switch to true.</param>
        /// <param name="searchPrefix">File name prefix used to filter the search.</param>
        /// <returns>
        ///   <see cref="IObservable{T}"/> of <see cref="BlobItem"/> all BLOB items in the path requested.
        /// </returns>
        public IObservable<IBlobItem> ListBlobsObservable(string rootFolder, bool recursive, bool fetchBlobAttributes = false, string searchPrefix = null)
        {
            var container = GetContainer(rootFolder, true).GetAwaiter().GetResult();
            var folderPath = rootFolder.Replace(container.Name, string.Empty);

            folderPath = ParsePath(folderPath);

            return Observable.Create<BlobItem>(async obs =>
            {
                BlobContinuationToken continuationToken = null;

                do
                {
                    BlobResultSegment response;

                    if (folderPath.Length == 0)
                    {
                        response = await container.ListBlobsSegmentedAsync(searchPrefix, recursive, fetchBlobAttributes ? BlobListingDetails.Metadata : BlobListingDetails.None,
                            100, continuationToken, new BlobRequestOptions
                            {
                                RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 5)
                            }, null).ConfigureAwait(false);
                    }
                    else
                    {
                        response = await container.GetDirectoryReference(folderPath).ListBlobsSegmentedAsync(recursive, BlobListingDetails.None, 100, continuationToken, new BlobRequestOptions
                        {
                            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 5)
                        }, null).ConfigureAwait(false);
                    }

                    continuationToken = response.ContinuationToken;
                    foreach (var item in response.Results)
                    {
                        // Only required if recursive == false
                        if (item.GetType() != typeof(CloudBlobDirectory))
                        {
                            if (item is CloudBlockBlob mappedItem)
                            {
                                // Don't think this is needed any more - will keep this here a while.
                                //// Grab additional attributes if type is CloudBlockBlob and fetchBlobAttribute has been signaled.
                                //if (fetchBlobAttributes)
                                //    await mappedItem.FetchAttributesAsync(new AccessCondition(), new BlobRequestOptions(), new OperationContext()).ConfigureAwait(false);
                                obs.OnNext(new BlobItem(mappedItem));
                            }
                            else
                                obs.OnNext(new BlobItem(item));
                        }
                    }
                }
                while (continuationToken != null);

                obs.OnCompleted();
            });
        }

        /// <summary>
        /// Copies the content of one directory to another on the server side.
        /// Avoids having to download all items and reupload them to somewhere else on the client side.
        /// </summary>
        /// <param name="sourceDirectoryPath">The path to the directory containing the files to be transferred</param>
        /// <param name="destinationDirectoryPath">The path to the destination directory </param>
        /// <param name="transferEvent">Action callback for tracking progress of each file when transferring.</param>
        /// <returns>Result from the  server side transfer.</returns>
        public async Task<ITransferResult> CopyDirectory(string sourceDirectoryPath, string destinationDirectoryPath, Action<Core.TransferEventType, ITransferEvent> transferEvent = null)
        {
            var sourceContainerName = GetContainerFromPath(sourceDirectoryPath);
            var destinationContainerName = GetContainerFromPath(destinationDirectoryPath);

            // Ensure destination folder exists if we've configured to create automatically.
            if (CreateFolderIfNotExists)
                await GetContainer(destinationContainerName, true).ConfigureAwait(false);

            var directoryTransferContext = new DirectoryTransferContext();

            // Subscribe to the transfer events if an action method was passed.
            if (transferEvent != null)
            {
                directoryTransferContext.FileTransferred += (fileTransferSender, fileTransferredEventArgs) =>
                {
                    ITransferEvent i = (TransferEvent)fileTransferredEventArgs;
                    transferEvent(Core.TransferEventType.Transferred, i);
                };

                directoryTransferContext.FileFailed += (fileFailedSender, fileTransferredEventArgs) =>
                {
                    ITransferEvent i = (TransferEvent)fileTransferredEventArgs;
                    transferEvent(Core.TransferEventType.Failed, i);
                };

                directoryTransferContext.FileSkipped += (fileSkippedSender, fileTransferredEventArgs) =>
                {
                    ITransferEvent i = (TransferEvent)fileTransferredEventArgs;
                    transferEvent(Core.TransferEventType.Skipped, i);
                };
            }

            directoryTransferContext.ShouldOverwriteCallbackAsync = (source, destination) => Task.FromResult(true);

            var copyOptions = new CopyDirectoryOptions { BlobType = BlobType.AppendBlob, Recursive = true };

            var sourceContainer = CloudBlobClient.GetContainerReference(sourceContainerName);
            var sourceRelativeUrl = GetPathWithoutContainer(sourceDirectoryPath);
            var sourceDirectory = sourceContainer.GetDirectoryReference(sourceRelativeUrl);

            var destinationContainer = CloudBlobClient.GetContainerReference(destinationContainerName);
            var destinationRelativeUrl = GetPathWithoutContainer(destinationDirectoryPath);
            var destinationDirectory = destinationContainer.GetDirectoryReference(destinationRelativeUrl);

            var transferTask = TransferManager.CopyDirectoryAsync(sourceDirectory, destinationDirectory, CopyMethod.ServiceSideSyncCopy, copyOptions, directoryTransferContext);
            var result = await transferTask;

            // Output the result from the transfer.
            return new TransferResult
            {
                BytesTransferred = result.BytesTransferred,
                NumberOfFilesFailed = result.NumberOfFilesFailed,
                NumberOfFilesSkipped = result.NumberOfFilesSkipped,
                NumberOfFilesTransferred = result.NumberOfFilesTransferred
            };
        }

        /// <summary>
        /// Deletes the container if it exists.
        /// </summary>
        /// <param name="containerName">The name of the container.</param>
        /// <returns></returns>
        public async Task DeleteContainer(string containerName)
        {
            var container = CloudBlobClient.GetContainerReference(containerName);
            await container.DeleteIfExistsAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            // Dispose of unmanaged resources.
            Dispose(true);
            // Suppress finalization.
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                // Free any other managed objects here.
                LockTimers.Release();
            }

            _disposed = true;
        }

        private string ParsePath(string folderPath)
        {
            if (folderPath.Length > 0)
            {
                return folderPath[0] == '/' ? folderPath.Substring(1, folderPath.Length - 1) : folderPath;
            }

            return string.Empty;
        }
    }

    /// <summary>
    /// Base class for Azure specific implementation of BLOB cloud storage.
    /// </summary>
    public abstract class BlobStorageBase
    {
        /// <summary>
        /// Holds a list of cached connection strings.
        /// </summary>
        internal static readonly ConcurrentDictionary<string, string> ConnectionStrings = new ConcurrentDictionary<string, string>();

        internal readonly ILogger Logger;
        internal readonly ServicePrincipleConfig ServicePrincipleConfig;
        internal readonly MsiConfig MsiConfig;
        internal readonly long LockTickInSeconds;
        internal string ConnectionString;
        internal readonly bool CreateFolderIfNotExists;

        private CloudBlobClient _cloudBlobClient;
        private DateTimeOffset? _expiryTime;
        private readonly string _instanceName;
        private readonly string _subscriptionId;

        private CloudBlobContainer _container;
        private string _basePath;

        /// <summary>Name of the object instance.</summary>
        public string Name { get; set; }

        public string BasePath
        {
            get { return _basePath; }
            set
            {
                _basePath = value;
                if (_basePath.IsNullOrEmpty())
                    _container = null;
                else
                    _container = GetContainer(_basePath).GetAwaiter().GetResult();
            }
        }

        internal CloudBlobClient CloudBlobClient
        {
            get
            {
                if (_cloudBlobClient == null || _expiryTime <= DateTime.UtcNow)
                    InitializeClient();

                return _cloudBlobClient;
            }
        }

        private void InitializeClient()
        {
            if (ConnectionString.IsNullOrEmpty())
                ConnectionString = BuildStorageConnection().GetAwaiter().GetResult();

            CloudStorageAccount.TryParse(ConnectionString, out var storageAccount);

            if (storageAccount == null)
                throw new InvalidOperationException("Cannot find storage account using connection string");

            // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
            _cloudBlobClient = storageAccount.CreateCloudBlobClient();
            _cloudBlobClient.DefaultRequestOptions.RetryPolicy = new LinearRetry(TimeSpan.FromMilliseconds(500), 3);
        }

        protected BlobStorageBase(ConnectionConfig config, ILogger logger = null)
        {
            // Ensure all mandatory fields are set.
            config.ThrowIfInvalid();

            Logger = logger;
            Name = config.InstanceName;
            CreateFolderIfNotExists = config.CreateFolderIfNotExists;
            LockTickInSeconds = (long)Math.Floor(config.LockInSeconds * 0.8); // renew at 80% lock-time to cope with load
            ConnectionString = config.ConnectionString;
        }

        protected BlobStorageBase(MsiConfig config, ILogger logger = null)
        {
            // Ensure all mandatory fields are set.
            config.ThrowIfInvalid();

            Logger = logger;
            CreateFolderIfNotExists = config.CreateFolderIfNotExists;
            LockTickInSeconds = (long)Math.Floor(config.LockInSeconds * 0.8); // renew at 80% lock-time to cope with load
            MsiConfig = config;
            Name = config.InstanceName;
            _instanceName = config.InstanceName;
            _subscriptionId = config.SubscriptionId;
        }

        protected BlobStorageBase(ServicePrincipleConfig config, ILogger logger = null)
        {
            // Ensure all mandatory fields are set.
            config.ThrowIfInvalid();

            Logger = logger;
            CreateFolderIfNotExists = config.CreateFolderIfNotExists;
            LockTickInSeconds = (long)Math.Floor(config.LockInSeconds * 0.8); // renew at 80% lock-time to cope with load
            ServicePrincipleConfig = config;
            Name = config.InstanceName;
            _instanceName = config.InstanceName;
            _subscriptionId = config.SubscriptionId;
        }

        /// <summary>
        /// Gets the block BLOB reference.
        /// </summary>
        /// <param name="blobPath">The BLOB path.</param>
        /// <returns><see cref="CloudBlockBlob"/> cloud BLOB reference.</returns>
        internal async Task<CloudBlockBlob> GetBlockBlobReference(string blobPath)
        {
            var container = await GetContainer(blobPath).ConfigureAwait(false);
            var blobName = GetBlobRelativePath(blobPath);

            return container.GetBlockBlobReference(blobName);
        }

        /// <summary>
        /// Gets the Azure BLOB Container.
        /// </summary>
        /// <param name="fullPath">The full path of the container.</param>
        /// <param name="createIfDoesNotExist">if set to <c>true</c> [create the container if it does not exist].</param>
        /// <returns><see cref="CloudBlobContainer"/></returns>
        /// <exception cref="InvalidOperationException">If the passed path starts with a "/" then an exception is thrown.</exception>
        internal async Task<CloudBlobContainer> GetContainer(string fullPath, bool createIfDoesNotExist = false)
        {
            if (_container != null)
                return _container;

            var containerName = GetContainerFromPath(fullPath);

            var container = CloudBlobClient.GetContainerReference(containerName);

            if (createIfDoesNotExist)
                await container.CreateIfNotExistsAsync().ConfigureAwait(false);

            return container;
        }

        /// <summary>
        /// Builds a connection string for the storage account when none was specified during initialisation.
        /// </summary>
        /// <returns>Connection <see cref="string"/></returns>
        /// <exception cref="InvalidOperationException">
        /// If the Storage Namespace can not be resolved or access keys are not configured.
        /// </exception>
        internal async Task<string> BuildStorageConnection()
        {
            try
            {
                // If we already have the connection string for this instance - don't go get it again.
                if (ConnectionStrings.TryGetValue(_instanceName, out var connStr))
                {
                    return connStr;
                }

                const string azureManagementAuthority = "https://management.azure.com/";
                const string windowsLoginAuthority = "https://login.windows.net/";
                string token;

                // Use Msi Config if it's been specified, otherwise, use Service principle.
                if (MsiConfig != null)
                {
                    // Managed Service Identity (MSI) authentication.
                    var provider = new AzureServiceTokenProvider();
                    token = provider.GetAccessTokenAsync(azureManagementAuthority, MsiConfig.TenantId).GetAwaiter().GetResult();

                    if (string.IsNullOrEmpty(token))
                        throw new InvalidOperationException("Could not authenticate using Managed Service Identity, ensure the application is running in a secure context");

                    _expiryTime = DateTime.Now.AddDays(1);
                }
                else
                {
                    // Service Principle authentication
                    // Grab an authentication token from Azure.
                    var context = new AuthenticationContext($"{windowsLoginAuthority}{ServicePrincipleConfig.TenantId}");

                    var credential = new ClientCredential(ServicePrincipleConfig.AppId, ServicePrincipleConfig.AppSecret);
                    var tokenResult = context.AcquireTokenAsync(azureManagementAuthority, credential).GetAwaiter().GetResult();

                    if (tokenResult == null || tokenResult.AccessToken == null)
                        throw new InvalidOperationException($"Could not authenticate to {windowsLoginAuthority}{ServicePrincipleConfig.TenantId} using supplied AppId: {ServicePrincipleConfig.AppId}");

                    _expiryTime = tokenResult.ExpiresOn;
                    token = tokenResult.AccessToken;
                }

                // Set credentials and grab the authenticated REST client.
                var tokenCredentials = new TokenCredentials(token);

                var client = RestClient.Configure()
                    .WithEnvironment(AzureEnvironment.AzureGlobalCloud)
                    .WithLogLevel(HttpLoggingDelegatingHandler.Level.BodyAndHeaders)
                    .WithCredentials(new AzureCredentials(tokenCredentials, tokenCredentials, string.Empty, AzureEnvironment.AzureGlobalCloud))
                    .WithRetryPolicy(new RetryPolicy(new HttpStatusCodeErrorDetectionStrategy(), new FixedIntervalRetryStrategy(3, TimeSpan.FromMilliseconds(500))))
                    .Build();

                // Authenticate against the management layer.
                var azureManagement = Azure.Authenticate(client, string.Empty).WithSubscription(_subscriptionId);

                // Get the storage namespace for the passed in instance name.
                var storageNamespace = azureManagement.StorageAccounts.List().FirstOrDefault(n => n.Name == _instanceName);

                // If we cant find that name, throw an exception.
                if (storageNamespace == null)
                {
                    throw new InvalidOperationException($"Could not find the storage instance {_instanceName} in the subscription Id specified");
                }

                // Storage accounts use access keys - this will be used to build a connection string.
                var accessKeys = await storageNamespace.GetKeysAsync().ConfigureAwait(false);

                // If the access keys are not found (not configured for some reason), throw an exception.
                if (accessKeys == null)
                {
                    throw new InvalidOperationException($"Could not find access keys for the storage instance {_instanceName}");
                }

                // We just default to the first key.
                var key = accessKeys[0].Value;

                // Build the connection string.
                var connectionString = $"DefaultEndpointsProtocol=https;AccountName={_instanceName};AccountKey={key};EndpointSuffix=core.windows.net";

                // Cache the connection string off so we don't have to reauthenticate.
                if (!ConnectionStrings.ContainsKey(_instanceName))
                {
                    ConnectionStrings.TryAdd(_instanceName, connectionString);
                }

                // Return connection string.
                return connectionString;
            }
            catch (Exception e)
            {
                _expiryTime = null;
                Logger?.LogError(e, "An exception occured during connection to blob storage");
                throw new InvalidOperationException("An exception occurred during service connection, see inner exception for more detail", e);
            }
        }

        /// <summary>
        /// Get a container name from full path, e.g. if "Container1/SubPath1/Subpath2" is passed, it will return "Container1".
        /// </summary>
        /// <param name="fullPath">Full path to parse, separated by forward slashes.</param>
        /// <returns>Gets the container name from a path.</returns>
        internal string GetContainerFromPath(string fullPath)
        {
            var indexOfFirstSlash = fullPath.IndexOf("/", StringComparison.InvariantCulture);

            if (indexOfFirstSlash == 0)
            {
                fullPath = fullPath.Substring(1, fullPath.Length - 1);
                indexOfFirstSlash = fullPath.IndexOf("/", StringComparison.InvariantCulture);
            }

            indexOfFirstSlash = (indexOfFirstSlash == -1 ? fullPath.Length : indexOfFirstSlash);

            // Return found container name.
            return fullPath.Substring(0, indexOfFirstSlash);
        }

        /// <summary>
        /// Gets the sub folder path from a full path e.g. if "Container1/subpath1/subpath2" is passed, it will return "subpath1/subpath2".
        /// </summary>
        /// <param name="fullPath">Full path to parse, separated by forward slashes.</param>
        /// <returns>Sub folder path after the container name</returns>
        internal string GetPathWithoutContainer(string fullPath)
        {
            // Get the container name from the path.
            var containerName = GetContainerFromPath(fullPath);

            // Remove the container name from the path.
            fullPath = fullPath.Replace(containerName, string.Empty);

            // Remove redundant forwardslash, if exists.
            if (fullPath.Length > 0 && fullPath[0] == '/')
                fullPath = fullPath.Remove(0, 1);

            return fullPath;
        }

        /// <summary>
        /// Gets the name of the BLOB file from the path e.g. "Container1/subfolder/blob1", it will return "blob1"
        /// </summary>
        /// <param name="path">The path to parse.</param>
        /// <returns><see cref="string"/> BLOB file name</returns>
        internal string GetBlobRelativePath(string path)
        {
            var firstSlashIndex = path.IndexOf("/", StringComparison.InvariantCulture);

            if (firstSlashIndex == 0)
            {
                path = path.Substring(1, path.Length - 1);
            }

            if (firstSlashIndex > -1)
            {
                var indexOfFirstSlash = path.IndexOf("/", StringComparison.InvariantCulture) + 1;
                return path.Substring(indexOfFirstSlash, path.Length - indexOfFirstSlash);
            }
            return path;
        }
    }
}
