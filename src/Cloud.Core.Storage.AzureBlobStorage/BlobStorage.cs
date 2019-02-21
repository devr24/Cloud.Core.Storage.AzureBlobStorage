namespace Cloud.Core.Storage.AzureBlobStorage
{
    using Config;
    using System.Reactive.Linq;
    using System.Collections.Concurrent;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using JetBrains.Annotations;
    using Microsoft.Azure.Management.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Microsoft.Rest;
    using Microsoft.Rest.TransientFaultHandling;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;
    using Microsoft.Azure.Services.AppAuthentication;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Azure specific implementation of BLOB cloud storage.
    /// </summary>
    /// <seealso cref="IBlobStorage" />
    /// <seealso cref="BlobStorageBase" />
    public class BlobStorage : BlobStorageBase, IBlobStorage
    {
        internal readonly IDictionary<IBlobItem, Timer> LockTimers = new ConcurrentDictionary<IBlobItem, Timer>(ObjectReferenceEqualityComparer<IBlobItem>.Default);

        /// <summary>
        /// Initializes a new instance of <see cref="BlobStorage" /> with Service Principle authentication.
        /// </summary>
        /// <param name="config">The Service Principle configuration settings for connecting to storage.</param>
        /// <param name="logger">The logger to log information to.</param>
        /// <inheritdoc />
        public BlobStorage([NotNull]ServicePrincipleConfig config, [CanBeNull] ILogger logger = null)
            : base(config, logger)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobStorage" /> class with a Connection String.
        /// </summary>
        /// <param name="config">The Connection String information for connecting to Storage.</param>
        /// <param name="logger">The logger.</param>
        public BlobStorage([NotNull]ConnectionConfig config, [CanBeNull] ILogger logger = null)
            : base(config, logger) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobStorage" /> class with Managed Service Identity (MSI) authentication.
        /// </summary>
        /// <param name="config">The Managed Service Identity (MSI) configuration for connecting to storage.</param>
        /// <param name="logger">The logger.</param>
        public BlobStorage([NotNull]MsiConfig config, [CanBeNull]ILogger logger = null)
            : base(config, logger) { }

        /// <summary>
        /// Lists all root level containers.
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<string>> ListFolders()
        {
            BlobContinuationToken continuationToken = null;
            var results = new List<string>();
            do
            {
                var response = await CloudBlobClient.ListContainersSegmentedAsync(null, continuationToken);
                continuationToken = response.ContinuationToken;
                results.AddRange(response.Results.Select(i => i.Name));
            }
            while (continuationToken != null);
            return results;
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
            await ((CloudBlockBlob)blob.Tag).DownloadToStreamAsync(stream);
            stream.Seek(0, SeekOrigin.Begin);
            return stream;
        }

        /// <summary>
        /// Uploads the BLOB to storage.
        /// </summary>
        /// <param name="blobPath">The BLOB path.</param>
        /// <param name="stream">The stream.</param>
        /// <returns>Async Task</returns>
        /// <inheritdoc />
        public async Task UploadBlob(string blobPath, Stream stream)
        {

            var cloudBlockBlob = await GetBlockBlobReference(blobPath).ConfigureAwait(false);
            await cloudBlockBlob.UploadFromStreamAsync(stream);
        }

        /// <summary>
        /// Uploads the BLOB to storage.
        /// </summary>
        /// <param name="blobPath">The BLOB path to upload to.</param>
        /// <param name="filePath">The file path to upload from.</param>
        /// <returns>Async Task</returns>
        /// <inheritdoc />
        public async Task UploadBlob(string blobPath, string filePath)
        {
            var cloudBlockBlob = await GetBlockBlobReference(blobPath).ConfigureAwait(false);
            await cloudBlockBlob.UploadFromFileAsync(filePath);
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
            await cloudBlockBlob.DeleteIfExistsAsync();
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
            folderPath = folderPath.Length > 0 ? folderPath[0] == '/' ? folderPath.Substring(1, folderPath.Length - 1) : folderPath : string.Empty;
            
            var ctoken = new BlobContinuationToken();
            do
            {
                var result = await container.ListBlobsSegmentedAsync(folderPath, true, BlobListingDetails.None, null, ctoken, null, null);
                ctoken = result.ContinuationToken;
                await Task.WhenAll(result.Results
                    .Select(item => (item as CloudBlob)?.DeleteIfExistsAsync())
                    .Where(task => task != null)
                );
            } while (ctoken != null);
        }

        /// <summary>
        /// Adds the folder.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns>Task.</returns>
        /// <exception cref="NotImplementedException"></exception>
        public async Task AddFolder(string path)
        {
            await GetContainer(path, true).ConfigureAwait(false);
        }

        /// <summary>Gets the BLOB with lock.</summary>
        /// <param name="blobPath">The BLOB path.</param>
        /// <param name="fetchAttributes">if set to <c>true</c> [fetch attributes].</param>
        /// <returns>Task IBlobItem.</returns>
        public async Task<IBlobItem> GetBlob(string blobPath, bool fetchAttributes = false)
        {
            var cloudBlockBlob = await GetBlockBlobReference(blobPath).ConfigureAwait(false);

            if (fetchAttributes)
                await cloudBlockBlob.FetchAttributesAsync(new AccessCondition(), new BlobRequestOptions(), new OperationContext());

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

            var blobItem = new BlobItem(cloudBlockBlob);

            if (!leaseName.IsNullOrEmpty())
                blobItem.UniqueLeaseName = leaseName;

            await cloudBlockBlob.AcquireLeaseAsync(TimeSpan.FromSeconds(ServicePrincipleConfig.LockInSeconds), blobItem.UniqueLeaseName).ConfigureAwait(false);

            LockTimers.Add(
                blobItem,
                new Timer(
                    async _ => {
                        AccessCondition accessCondition = new AccessCondition { LeaseId = blobItem.UniqueLeaseName };
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
                CloudBlockBlob tag = (CloudBlockBlob)item.Tag;

                AccessCondition accessCondition = new AccessCondition { LeaseId = item.UniqueLeaseName };
                await tag.ReleaseLeaseAsync(accessCondition);
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
        /// <param name="path">The path to gather the blob items from.</param>
        /// <param name="recursive">Will traverse sub folders if [recursive] is <c>true</c>.</param>
        /// <returns>
        ///   <see cref="System.Collections.IEnumerable" /> of <see cref="BlobItem" /> all BLOB items in the path requested.
        /// </returns>
        public IEnumerable<IBlobItem> ListBlobs(string path, bool recursive)
        {
            return ListBlobsObservable(path, recursive).ToEnumerable();
        }

        /// <summary>
        /// Lists the blobs observable.
        /// </summary>
        /// <param name="path">The path to gather the blob items from.</param>
        /// <param name="recursive">Will traverse sub folders if [recursive] is <c>true</c>.</param>
        /// <returns>
        ///   <see cref="IObservable{T}"/> of <see cref="BlobItem"/> all BLOB items in the path requested.
        /// </returns>
        public IObservable<IBlobItem> ListBlobsObservable(string path, bool recursive)
        {
            var container = GetContainer(path, true).GetAwaiter().GetResult();
            var folderPath = path.Replace(container.Name, string.Empty);
            folderPath = folderPath.Length > 0 ? folderPath[0] == '/' ? folderPath.Substring(1, folderPath.Length - 1) : folderPath : string.Empty;

            return Observable.Create<BlobItem>(async obs =>
            {
                BlobContinuationToken continuationToken = null;

                do
                {
                    BlobResultSegment response;

                    if (folderPath.Length == 0)
                    {
                        response = await container.ListBlobsSegmentedAsync(null, recursive, BlobListingDetails.None,
                            100, continuationToken, new BlobRequestOptions
                            {
                                RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 5)
                            }, null);
                    }
                    else
                    {
                        response = await container.GetDirectoryReference(folderPath).ListBlobsSegmentedAsync(recursive, BlobListingDetails.None, 100, continuationToken, new BlobRequestOptions
                        {
                            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 5)
                        }, null);
                    }

                    continuationToken = response.ContinuationToken;
                    foreach (var item in response.Results)
                    {
                        // Only required if recursive == false
                        if (item.GetType() != typeof(CloudBlobDirectory))
                            obs.OnNext(new BlobItem(item));
                    }
                }
                while (continuationToken != null);
            });
        }

        /// <inheritdoc />
        public void Dispose()
        {
            LockTimers.Release();
        }
    }

    /// <summary>
    /// Base class for Azure specific implementation of BLOB cloud storage.
    /// </summary>
    public abstract class BlobStorageBase
    {
        internal readonly ILogger Logger;
        internal readonly ServicePrincipleConfig ServicePrincipleConfig;
        internal readonly MsiConfig MsiConfig;
        internal readonly long LockTickInSeconds;
        internal string ConnectionString;

        private CloudBlobClient _cloudBlobClient;
        private DateTimeOffset? _expiryTime;
        private readonly string _instanceName;
        private readonly string _subscriptionId;

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

        internal BlobStorageBase(ConnectionConfig config, ILogger logger = null)
        {
            // Ensure all mandatory fields are set.
            config.Validate();

            Logger = logger;
            LockTickInSeconds = (long)Math.Floor(60 * 0.8); // renew at 80% lock-time to cope with load
            ConnectionString = config.ConnectionString;
        }

        internal BlobStorageBase(MsiConfig config, ILogger logger = null)
        {
            // Ensure all mandatory fields are set.
            config.Validate();

            Logger = logger;
            LockTickInSeconds = (long)Math.Floor(60 * 0.8); // renew at 80% lock-time to cope with load
            MsiConfig = config;
            _instanceName = config.StorageInstanceName;
            _subscriptionId = config.SubscriptionId;
        }
        
        internal BlobStorageBase(ServicePrincipleConfig config, ILogger logger = null)
        {
            // Ensure all mandatory fields are set.
            config.Validate();

            Logger = logger;
            LockTickInSeconds = (long)Math.Floor(config.LockInSeconds * 0.8); // renew at 80% lock-time to cope with load
            ServicePrincipleConfig = config;
            _instanceName = config.StorageInstanceName;
            _subscriptionId = config.SubscriptionId;
        }

        /// <summary>
        /// Gets the block BLOB reference.
        /// </summary>
        /// <param name="blobPath">The BLOB path.</param>
        /// <returns><see cref="CloudBlockBlob"/> cloud BLOB reference.</returns>
        internal async Task<CloudBlockBlob> GetBlockBlobReference(string blobPath)
        {
            var container = await GetContainer(blobPath);
            var blobName = GetBlobName(blobPath);

            return container.GetBlockBlobReference(blobName);
        }

        /// <summary>
        /// Gets the name of the BLOB file from the path.
        /// </summary>
        /// <param name="path">The path to parse.</param>
        /// <returns><see cref="string"/> BLOB file name</returns>
        internal string GetBlobName(string path)
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

        /// <summary>
        /// Gets the Azure BLOB Container.
        /// </summary>
        /// <param name="fullPath">The full path of the container.</param>
        /// <param name="createIfDoesNotExist">if set to <c>true</c> [create the container if it does not exist].</param>
        /// <returns><see cref="CloudBlobContainer"/></returns>
        /// <exception cref="InvalidOperationException">If the passed path starts with a "/" then an exception is thrown.</exception>
        internal async Task<CloudBlobContainer> GetContainer(string fullPath, bool createIfDoesNotExist = false)
        {
            var indexOfFirstSlash = fullPath.IndexOf("/", StringComparison.InvariantCulture);

            if (indexOfFirstSlash == 0)
            {
                fullPath = fullPath.Substring(1, fullPath.Length - 1);
                indexOfFirstSlash = fullPath.IndexOf("/", StringComparison.InvariantCulture);
            }

            indexOfFirstSlash = indexOfFirstSlash == -1 ? fullPath.Length : indexOfFirstSlash;

            var containerName = fullPath.Substring(0, indexOfFirstSlash);
            var container = CloudBlobClient.GetContainerReference(containerName);

            if (createIfDoesNotExist)
                await container.CreateIfNotExistsAsync();

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
                var accessKeys = await storageNamespace.GetKeysAsync();

                // If the access keys are not found (not configured for some reason), throw an exception.
                if (accessKeys == null)
                {
                    throw new InvalidOperationException($"Could not find access keys for the storage instance {_instanceName}");
                }

                // We just default to the first key.
                var key = accessKeys[0].Value;

                // Build and return the connection string.
                return $"DefaultEndpointsProtocol=https;AccountName={_instanceName};AccountKey={key};EndpointSuffix=core.windows.net";
            }
            catch (Exception e)
            {
                _expiryTime = null;
                Logger?.LogError(e, "An exception occured during connection to blob storage");
                throw new Exception("An exception occurred during service connection, see inner exception for more detail", e);
            }
        }
    }
}
