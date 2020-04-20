using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;

namespace Cloud.Core.Storage.AzureBlobStorage.Tests.Unit
{

    public class MockBlobStorage : CloudBlobClient
    {
        public MockBlobStorage(Uri baseUri) : base(baseUri)
        {
        }

        public MockBlobStorage(Uri baseUri, StorageCredentials credentials) : base(baseUri, credentials)
        {
        }

        public MockBlobStorage(StorageUri storageUri, StorageCredentials credentials) : base(storageUri, credentials)
        {
        }

        public virtual CloudBlobClient CreateCloudBlobClient()
        {
            return new MockBlobStorage(new Uri("http://test.com"));
        }

        public virtual MockBlockBlob GetBlockBlobReference(string blobName)
        {
            return new MockBlockBlob(new Uri("http://test.com"));
        }
        public override Task<ContainerResultSegment> ListContainersSegmentedAsync(string prefix, BlobContinuationToken currentToken)
        {
            return Task.FromResult(new ContainerResultSegment(new List<MockContainer>(), currentToken));
        }

        public virtual Task DownloadToStreamAsync(Stream target)
        {
            using (var writer = new StreamWriter(target))
            {
                writer.WriteAsync("This is a test");
                return writer.FlushAsync();
            }
        }

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

        public virtual Task UploadFromStreamAsync(Stream source)
        {
            return Task.FromResult(true);
        }

        public virtual Task UploadFromFileAsync(string path)
        {
            return Task.FromResult(true);
        }

        public virtual Task<bool> DeleteIfExistsAsync()
        {
            return Task.FromResult(true);
        }

        public virtual Task<string> AcquireLeaseAsync(TimeSpan? leaseTime, string proposedLeaseId = null)
        {
            return Task.FromResult(proposedLeaseId);
        }

        public virtual Task ReleaseLeaseAsync(AccessCondition accessCondition)
        {
            return Task.FromResult(true);
        }

        public virtual Task<BlobResultSegment> ListBlobsSegmentedAsync(bool useFlatBlobListing,
            BlobListingDetails blobListingDetails, int? maxResults, BlobContinuationToken currentToken,
            BlobRequestOptions options, OperationContext operationContext)
        {
            return Task.FromResult(new BlobResultSegment(new List<IListBlobItem>(), currentToken));
        }

    }

    public class MockContainer : CloudBlobContainer
    {
        public MockContainer(Uri containerAddress) : base(containerAddress)
        {
        }

        public MockContainer(Uri containerAddress, StorageCredentials credentials) : base(containerAddress, credentials)
        {
        }

        public MockContainer(StorageUri containerAddress, StorageCredentials credentials) : base(containerAddress, credentials)
        {
        }

        public virtual MockBlockBlob GetContainerReference(string containerName)
        {
            return new MockBlockBlob(new Uri("http://test.com"));
        }
    }

    public class MockBlockBlob: CloudBlockBlob
    {
        public MockBlockBlob(Uri blobAbsoluteUri) : base(blobAbsoluteUri)
        {
        }

        public MockBlockBlob(Uri blobAbsoluteUri, StorageCredentials credentials) : base(blobAbsoluteUri, credentials)
        {
        }

        public MockBlockBlob(Uri blobAbsoluteUri, CloudBlobClient client) : base(blobAbsoluteUri, client)
        {
        }

        public MockBlockBlob(Uri blobAbsoluteUri, DateTimeOffset? snapshotTime, StorageCredentials credentials) : base(blobAbsoluteUri, snapshotTime, credentials)
        {
        }

        public MockBlockBlob(Uri blobAbsoluteUri, DateTimeOffset? snapshotTime, CloudBlobClient client) : base(blobAbsoluteUri, snapshotTime, client)
        {
        }

        public MockBlockBlob(StorageUri blobAbsoluteUri, DateTimeOffset? snapshotTime, StorageCredentials credentials) : base(blobAbsoluteUri, snapshotTime, credentials)
        {
        }

        public MockBlockBlob(StorageUri blobAbsoluteUri, DateTimeOffset? snapshotTime, CloudBlobClient client) : base(blobAbsoluteUri, snapshotTime, client)
        {
        }

        public override Task<bool> DeleteIfExistsAsync()
        {
            return Task.FromResult(true);
        }
    }
}
