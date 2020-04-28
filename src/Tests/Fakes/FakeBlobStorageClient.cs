using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;

namespace Cloud.Core.Storage.AzureBlobStorage.Tests.Unit
{

    /// <summary>
    /// Class FakeBlobStorage.
    /// Implements the <see cref="Microsoft.Azure.Storage.Blob.CloudBlobClient" />
    /// </summary>
    /// <seealso cref="Microsoft.Azure.Storage.Blob.CloudBlobClient" />
    public class FakeBlobStorage : CloudBlobClient
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FakeBlobStorage"/> class.
        /// </summary>
        /// <param name="baseUri">The base URI.</param>
        public FakeBlobStorage(Uri baseUri) : base(baseUri)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FakeBlobStorage"/> class.
        /// </summary>
        /// <param name="baseUri">The base URI.</param>
        /// <param name="credentials">The credentials.</param>
        public FakeBlobStorage(Uri baseUri, StorageCredentials credentials) : base(baseUri, credentials)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FakeBlobStorage"/> class.
        /// </summary>
        /// <param name="storageUri">The storage URI.</param>
        /// <param name="credentials">The credentials.</param>
        public FakeBlobStorage(StorageUri storageUri, StorageCredentials credentials) : base(storageUri, credentials)
        {
        }

        /// <summary>
        /// Creates the cloud BLOB client.
        /// </summary>
        /// <returns>CloudBlobClient.</returns>
        public virtual CloudBlobClient CreateCloudBlobClient()
        {
            return new FakeBlobStorage(new Uri("http://test.com"));
        }

        /// <summary>
        /// Gets the block BLOB reference.
        /// </summary>
        /// <param name="blobName">Name of the BLOB.</param>
        /// <returns>FakeBlockBlob.</returns>
        public virtual FakeBlockBlob GetBlockBlobReference(string blobName)
        {
            return new FakeBlockBlob(new Uri("http://test.com"));
        }
        /// <summary>
        /// Lists the containers segmented asynchronous.
        /// </summary>
        /// <param name="prefix">The prefix.</param>
        /// <param name="currentToken">The current token.</param>
        /// <returns>Task&lt;ContainerResultSegment&gt;.</returns>
        public override Task<ContainerResultSegment> ListContainersSegmentedAsync(string prefix, BlobContinuationToken currentToken)
        {
            return Task.FromResult(new ContainerResultSegment(new List<FakeContainer>(), currentToken));
        }

        /// <summary>
        /// Downloads to stream asynchronous.
        /// </summary>
        /// <param name="target">The target.</param>
        /// <returns>Task.</returns>
        public virtual Task DownloadToStreamAsync(Stream target)
        {
            using (var writer = new StreamWriter(target))
            {
                writer.WriteAsync("This is a test");
                return writer.FlushAsync();
            }
        }

        /// <summary>
        /// Downloads to file asynchronous.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="mode">The mode.</param>
        /// <returns>Task.</returns>
        public virtual Task DownloadToFileAsync(string path, FileMode mode)
        {
            using (var fileStream = new MemoryStream())
            {
                using (var writer = new StreamWriter(fileStream))
                {
                    writer.Write("This is a test");
                    writer.Flush();

                    File.WriteAllBytes(path, fileStream.GetBuffer());
                    return Task.FromResult(true);
                }
            }
        }

        /// <summary>
        /// Uploads from stream asynchronous.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <returns>Task.</returns>
        public virtual Task UploadFromStreamAsync(Stream source)
        {
            return Task.FromResult(true);
        }

        /// <summary>
        /// Uploads from file asynchronous.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns>Task.</returns>
        public virtual Task UploadFromFileAsync(string path)
        {
            return Task.FromResult(true);
        }

        /// <summary>
        /// Deletes if exists asynchronous.
        /// </summary>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public virtual Task<bool> DeleteIfExistsAsync()
        {
            return Task.FromResult(true);
        }

        /// <summary>
        /// Acquires the lease asynchronous.
        /// </summary>
        /// <param name="leaseTime">The lease time.</param>
        /// <param name="proposedLeaseId">The proposed lease identifier.</param>
        /// <returns>Task&lt;System.String&gt;.</returns>
        public virtual Task<string> AcquireLeaseAsync(TimeSpan? leaseTime, string proposedLeaseId = null)
        {
            return Task.FromResult(proposedLeaseId);
        }

        /// <summary>
        /// Releases the lease asynchronous.
        /// </summary>
        /// <param name="accessCondition">The access condition.</param>
        /// <returns>Task.</returns>
        public virtual Task ReleaseLeaseAsync(AccessCondition accessCondition)
        {
            return Task.FromResult(true);
        }

        /// <summary>
        /// Lists the blobs segmented asynchronous.
        /// </summary>
        /// <param name="useFlatBlobListing">if set to <c>true</c> [use flat BLOB listing].</param>
        /// <param name="blobListingDetails">The BLOB listing details.</param>
        /// <param name="maxResults">The maximum results.</param>
        /// <param name="currentToken">The current token.</param>
        /// <param name="options">The options.</param>
        /// <param name="operationContext">The operation context.</param>
        /// <returns>Task&lt;BlobResultSegment&gt;.</returns>
        public virtual Task<BlobResultSegment> ListBlobsSegmentedAsync(bool useFlatBlobListing,
            BlobListingDetails blobListingDetails, int? maxResults, BlobContinuationToken currentToken,
            BlobRequestOptions options, OperationContext operationContext)
        {
            return Task.FromResult(new BlobResultSegment(new List<IListBlobItem>(), currentToken));
        }

    }

    /// <summary>
    /// Class FakeContainer.
    /// Implements the <see cref="Microsoft.Azure.Storage.Blob.CloudBlobContainer" />
    /// </summary>
    /// <seealso cref="Microsoft.Azure.Storage.Blob.CloudBlobContainer" />
    public class FakeContainer : CloudBlobContainer
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FakeContainer"/> class.
        /// </summary>
        /// <param name="containerAddress">A <see cref="T:System.Uri" /> object specifying the absolute URI to the container.</param>
        public FakeContainer(Uri containerAddress) : base(containerAddress)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FakeContainer"/> class.
        /// </summary>
        /// <param name="containerAddress">A <see cref="T:System.Uri" /> object specifying the absolute URI to the container.</param>
        /// <param name="credentials">A <see cref="T:Microsoft.Azure.Storage.Auth.StorageCredentials" /> object.</param>
        public FakeContainer(Uri containerAddress, StorageCredentials credentials) : base(containerAddress, credentials)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FakeContainer"/> class.
        /// </summary>
        /// <param name="containerAddress">A <see cref="T:System.Uri" /> object specifying the absolute URI to the container.</param>
        /// <param name="credentials">A <see cref="T:Microsoft.Azure.Storage.Auth.StorageCredentials" /> object.</param>
        public FakeContainer(StorageUri containerAddress, StorageCredentials credentials) : base(containerAddress, credentials)
        {
        }

        /// <summary>
        /// Gets the container reference.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <returns>FakeBlockBlob.</returns>
        public virtual FakeBlockBlob GetContainerReference(string containerName)
        {
            return new FakeBlockBlob(new Uri("http://test.com"));
        }
    }

    /// <summary>
    /// Class FakeBlockBlob.
    /// Implements the <see cref="Microsoft.Azure.Storage.Blob.CloudBlockBlob" />
    /// </summary>
    /// <seealso cref="Microsoft.Azure.Storage.Blob.CloudBlockBlob" />
    public class FakeBlockBlob: CloudBlockBlob
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FakeBlockBlob"/> class.
        /// </summary>
        /// <param name="blobAbsoluteUri">A <see cref="T:System.Uri" /> specifying the absolute URI to the blob.</param>
        public FakeBlockBlob(Uri blobAbsoluteUri) : base(blobAbsoluteUri)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FakeBlockBlob"/> class.
        /// </summary>
        /// <param name="blobAbsoluteUri">A <see cref="T:System.Uri" /> specifying the absolute URI to the blob.</param>
        /// <param name="credentials">A <see cref="T:Microsoft.Azure.Storage.Auth.StorageCredentials" /> object.</param>
        public FakeBlockBlob(Uri blobAbsoluteUri, StorageCredentials credentials) : base(blobAbsoluteUri, credentials)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FakeBlockBlob"/> class.
        /// </summary>
        /// <param name="blobAbsoluteUri">A <see cref="T:System.Uri" /> specifying the absolute URI to the blob.</param>
        /// <param name="client">A <see cref="T:Microsoft.Azure.Storage.Blob.CloudBlobClient" /> object.</param>
        public FakeBlockBlob(Uri blobAbsoluteUri, CloudBlobClient client) : base(blobAbsoluteUri, client)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FakeBlockBlob"/> class.
        /// </summary>
        /// <param name="blobAbsoluteUri">A <see cref="T:System.Uri" /> specifying the absolute URI to the blob.</param>
        /// <param name="snapshotTime">A <see cref="T:System.DateTimeOffset" /> specifying the snapshot timestamp, if the blob is a snapshot.</param>
        /// <param name="credentials">A <see cref="T:Microsoft.Azure.Storage.Auth.StorageCredentials" /> object.</param>
        public FakeBlockBlob(Uri blobAbsoluteUri, DateTimeOffset? snapshotTime, StorageCredentials credentials) : base(blobAbsoluteUri, snapshotTime, credentials)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FakeBlockBlob"/> class.
        /// </summary>
        /// <param name="blobAbsoluteUri">A <see cref="T:System.Uri" /> specifying the absolute URI to the blob.</param>
        /// <param name="snapshotTime">A <see cref="T:System.DateTimeOffset" /> specifying the snapshot timestamp, if the blob is a snapshot.</param>
        /// <param name="client">A <see cref="T:Microsoft.Azure.Storage.Blob.CloudBlobClient" /> object.</param>
        public FakeBlockBlob(Uri blobAbsoluteUri, DateTimeOffset? snapshotTime, CloudBlobClient client) : base(blobAbsoluteUri, snapshotTime, client)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FakeBlockBlob"/> class.
        /// </summary>
        /// <param name="blobAbsoluteUri">A <see cref="T:Microsoft.Azure.Storage.StorageUri" /> containing the absolute URI to the blob at both the primary and secondary locations.</param>
        /// <param name="snapshotTime">A <see cref="T:System.DateTimeOffset" /> specifying the snapshot timestamp, if the blob is a snapshot.</param>
        /// <param name="credentials">A <see cref="T:Microsoft.Azure.Storage.Auth.StorageCredentials" /> object.</param>
        public FakeBlockBlob(StorageUri blobAbsoluteUri, DateTimeOffset? snapshotTime, StorageCredentials credentials) : base(blobAbsoluteUri, snapshotTime, credentials)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FakeBlockBlob"/> class.
        /// </summary>
        /// <param name="blobAbsoluteUri">A <see cref="T:Microsoft.Azure.Storage.StorageUri" /> containing the absolute URI to the blob at both the primary and secondary locations.</param>
        /// <param name="snapshotTime">A <see cref="T:System.DateTimeOffset" /> specifying the snapshot timestamp, if the blob is a snapshot.</param>
        /// <param name="client">A <see cref="T:Microsoft.Azure.Storage.Blob.CloudBlobClient" /> object.</param>
        public FakeBlockBlob(StorageUri blobAbsoluteUri, DateTimeOffset? snapshotTime, CloudBlobClient client) : base(blobAbsoluteUri, snapshotTime, client)
        {
        }

        /// <summary>
        /// Initiates an asynchronous operation to delete the blob if it already exists.
        /// </summary>
        /// <returns>A <see cref="T:System.Threading.Tasks.Task`1" /> object of type <c>bool</c> that represents the asynchronous operation.</returns>
        public override Task<bool> DeleteIfExistsAsync()
        {
            return Task.FromResult(true);
        }
    }
}
