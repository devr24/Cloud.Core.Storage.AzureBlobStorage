using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Cloud.Core.Storage.AzureBlobStorage.Config;
using Cloud.Core.Testing;
using Cloud.Core.Testing.Lorem;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Cloud.Core.Storage.AzureBlobStorage.Tests.Integration
{
    using TransferEventType = Core.TransferEventType;

    [IsIntegration]
    public class BlobStorageIntegrationTests : IDisposable
    {
        private readonly BlobStorage _client;
        private readonly ServicePrincipleConfig _config;
        private readonly ILogger _logger;
        private const string TestContainerName = "testing";
        private const string TestFileName = "testfile.txt";
        private readonly string _lease = "751C2444-F2C4-484D-8E88-B06FBDF8B4B8";
        private readonly string _fullPath = $"{TestContainerName}/{TestFileName}";

        public BlobStorageIntegrationTests()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            _config = new ServicePrincipleConfig
            {
                InstanceName = config.GetValue<string>("StorageInstanceName"),
                TenantId = config.GetValue<string>("TenantId"),
                SubscriptionId = config.GetValue<string>("SubscriptionId"),
                AppId = config.GetValue<string>("AppId"),
                AppSecret = config.GetValue<string>("AppSecret"),
                LockInSeconds = 60,
                CreateFolderIfNotExists = true
            };

            _logger = new ServiceCollection().AddLogging(builder => builder.AddConsole())
                .BuildServiceProvider().GetService<ILogger<BlobStorageIntegrationTests>>();

            _client = new BlobStorage(_config, _logger);
            _client.Name.Should().Be(config.GetValue<string>("StorageInstanceName"));
            RemoveTestFile(_fullPath);
        }

        [Fact]
        public void Test_BlobStorage_CachedConnectionString()
        {
            var targetDir = Environment.CurrentDirectory + "\\test.txt";
            TearUpDown((size, name) =>
            {
                _client.DownloadBlob(_fullPath, targetDir).GetAwaiter().GetResult();

                Assert.True(File.Exists(targetDir));

                _client.ConnectionString.Should().NotBeNullOrEmpty();

            });

            var testInstance = new BlobStorage(_config);
            var internalClient = testInstance.CloudBlobClient; // initialise the client connection.
            testInstance.ConnectionString.Should().NotBeNullOrEmpty();
        }

        /// <summary>Defines the test method for checking msi authentication raises erros when not set correctly.</summary>
        [Fact]
        public void Test_BlobStorage_MsiConfig()
        {
            var client = new BlobStorage(new MsiConfig { InstanceName = "test", SubscriptionId = "test", TenantId = "test" });
            Assert.Throws<InvalidOperationException>(() => client.BuildStorageConnection().GetAwaiter().GetResult());
        }

        [Fact]
        public void Test_BlobStorage_GetBlobExistsAndNotExists()
        {
            TearUpDown((size, name) =>
            {
                // Get the blob with a lock.
                var blobExists = _client.GetBlob(_fullPath).GetAwaiter().GetResult();
                blobExists.Should().NotBeNull();

                var blobDoesNotExist = _client.GetBlob($"{TestContainerName}/DOESNOTEXIST").GetAwaiter().GetResult();
                blobDoesNotExist.Should().BeNull();
            });
        }

        /// <summary>Verifies locks can be applied and removed on blob items.</summary>
        [Fact]
        public void Test_BlobStorage_GetBlobLockUnlock()
        {
            TearUpDown((size, name) =>
            {
                // Get the blob with a lock.
                var blob = _client.GetBlobWithLock(_fullPath, _lease).GetAwaiter().GetResult();
                Thread.Sleep(15);
                _client.UnlockBlob(blob);
                var content = _client.DownloadBlob(blob).GetAwaiter().GetResult();

                Assert.NotNull(content);
                Assert.True(blob.FileName == TestFileName);
                Assert.True(blob.FileNameWithoutExtension == "testfile");
                Assert.True(blob.FileExtension == "txt");
                Assert.True(blob.RootFolder == TestContainerName);
            });
        }

        /// <summary>Verifies a file can be uploaded to storage.</summary>
        [Fact]
        public void Test_BlobStorage_UploadFile()
        {
            // Test a file can be created and removed without exception.
            AssertExtensions.DoesNotThrow(() =>
            {
                TearUpDown((size, name) => { });
            });
        }

        /// <summary>Verifies a file can be downloaded from storage and saved to disk.</summary>
        [Fact]
        public void Test_BlobStorage_DownloadFileToDisk()
        {
            TearUpDown((size, name) =>
            {
                var targetDir = Environment.CurrentDirectory + "\\test.txt";
                _client.DownloadBlob(_fullPath, targetDir).GetAwaiter().GetResult();

                Assert.True(File.Exists(targetDir));
            });
        }

        /// <summary>Verifies a file can be downloaded into a memory stream.</summary>
        [Fact]
        public void Test_BlobStorage_DownloadFileStream()
        {
            TearUpDown((size, name) =>
            {
                var blob = _client.GetBlob(_fullPath, true).GetAwaiter().GetResult();

                string text;
                long fileSize;
                using (var stream = _client.DownloadBlob(blob).GetAwaiter().GetResult())
                {
                    Assert.NotNull(stream);
                    fileSize = stream.Length;
                    Assert.Equal(size, fileSize);

                    using (var reader = new StreamReader(stream))
                    {
                        text = reader.ReadToEnd();
                        Assert.Equal("This is a test this is a test", text);
                    }
                }

                var targetDir = Environment.CurrentDirectory + "\\test.txt";
                _client.DownloadBlob(_fullPath, targetDir).GetAwaiter().GetResult();

                Assert.True(File.Exists(targetDir));
            });
        }

        /// <summary>Verifies base path can be set for efficiency.</summary>
        [Fact]
        public void Test_BlobStorage_SetBasePath()
        {
            _client.BasePath = TestContainerName;

            TearUpDown((size, name) =>
            {
                _client.Exists(TestFileName).GetAwaiter().GetResult().Should().BeTrue();
            });

            _client.Exists(TestFileName).GetAwaiter().GetResult().Should().BeFalse();
        }

        /// <summary>Confirms blob exists returns the correct result.</summary>
        [Fact]
        public void Test_BlobStorage_BlobExists()
        {
            TearUpDown((size, name) =>
            {
                _client.Exists(_fullPath).GetAwaiter().GetResult().Should().BeTrue();
            });

            _client.Exists(_fullPath).GetAwaiter().GetResult().Should().BeFalse();
        }

        /// <summary>Verifies upload followed by download to disk works as expected when called together.</summary>
        [Fact]
        public void Test_BlobStorage_UploadxDownloadToDisk()
        {
            var localFilePath = $"{AppDomain.CurrentDomain.BaseDirectory}\\{TestFileName}";

            File.WriteAllText(localFilePath, Lorem.GetParagraph());
            _client.UploadBlob(_fullPath, localFilePath).GetAwaiter().GetResult();
            File.Delete(localFilePath);
            _client.DownloadBlob(_fullPath, localFilePath).GetAwaiter().GetResult();
            Assert.True(File.Exists(localFilePath));
            File.Delete(localFilePath);
        }

        /// <summary>Verifies listing the files works as expected.</summary>
        [Fact]
        public void Test_BlobStorage_ListFiles()
        {
            TearUpDown((size, name) =>
            {
                var files = _client.ListBlobs(TestContainerName, false);
                Assert.True(files.Select(f => f.FileName == TestFileName).Any());
            });
        }

        /// <summary>Verifies listing the folders (containers) works as expected.</summary>
        [Fact]
        public void Test_BlobStorage_ListFolders()
        {
            TearUpDown((size, name) =>
            {
                var folders = _client.ListFolders().GetAwaiter().GetResult();
                Assert.True(folders.Any());
            });
        }

        /// <summary>Verifies a file with attributes can be downloaded and attributes read.</summary>
        [Fact]
        public void Test_BlobStorage_GetBlobWithAttributes()
        {
            TearUpDown((size, name) =>
            {
                var localFilePath = $"{AppDomain.CurrentDomain.BaseDirectory}\\{TestFileName}";

                var blob = (BlobItem)_client.GetBlob(_fullPath, true).GetAwaiter().GetResult();

                var content = _client.DownloadBlob(blob).GetAwaiter().GetResult();
                File.WriteAllBytes(localFilePath, ((MemoryStream)content).GetBuffer());
                Assert.Equal(size, blob.FileSize);
                Assert.True(File.Exists(localFilePath));
                Assert.NotNull(content);
                Assert.True(blob.FileName == TestFileName);
                Assert.True(blob.RootFolder == TestContainerName);
                Assert.True(blob.FileExtension == "txt");
                Assert.True(blob.Metadata.ContainsKey("testKey"));
                Assert.True(blob.Metadata.ContainsValue("testValue"));
            });
        }

        /// <summary>Verifies observable way to pull lists of files.</summary>
        [Fact]
        public void Test_BlobStorage_ListFilesSubscribe()
        {
            TearUpDown((size, name) =>
            {
                _client.ListBlobsObservable(TestContainerName, true, true).Take(1).Subscribe(blobItem =>
                {
                    Assert.True(blobItem.FileName == TestFileName);
                });
            });
        }

        /// <summary>Verifies content hash of file.</summary>
        [Fact]
        public void Test_BlobStorage_GetBlobWithAttributesParsesContentHashCorrectlyAsync()
        {
            TearUpDown((size, name) =>
            {
                var azureBlob = _client.GetBlockBlobReference(_fullPath).GetAwaiter().GetResult();
                azureBlob.FetchAttributesAsync().GetAwaiter().GetResult();
                var azureContentMd5 = Convert.FromBase64String(azureBlob.Properties.ContentMD5);
                var decodedAzureContentHash = BitConverter.ToString(azureContentMd5).Replace("-", string.Empty).ToUpper();

                var aiCoreBlob = _client.GetBlob(_fullPath, true).GetAwaiter().GetResult();

                Assert.True(decodedAzureContentHash == aiCoreBlob.ContentHash);
                Assert.True(aiCoreBlob.ContentHash.IsEquivalentTo(aiCoreBlob.ContentHash.ToUpper()));

                var rg = new Regex(@"^[a-zA-Z0-9\s,]*$");
                Assert.Matches(rg, aiCoreBlob.ContentHash);
            });
        }

        /// <summary>Verifies blobs can be listed based off a prefix</summary>
        [Fact]
        public void Test_BlobStorage_ListFilesWithPrefix()
        {
            var metaData = new Dictionary<string, string>
            {
                { "testKey", "testValue" }
            };

            //Create blobs
            for (int i = 1; i <= 11; i++)
            {
                string filePath = $"{TestContainerName}/listtestfile{i}";

                var stringInMemoryStream = new MemoryStream(Encoding.Default.GetBytes("This is a test this is a test"));
                _client.UploadBlob(filePath, stringInMemoryStream, metaData).GetAwaiter().GetResult();
            }

            //Get Blobs
            var blobsWithPrefix = _client.ListBlobs(TestContainerName, false, false, "listtestfile1").ToList();

            //Cleanup Blobs
            for (int i = 1; i <= 11; i++)
            {
                string filePath = $"{TestContainerName}/listtestfile{i}";

                var blob = _client.GetBlob(filePath).GetAwaiter().GetResult();
                ((BlobItem)blob).UniqueLeaseName = _lease;
                _client.UnlockBlob(blob);
                _client.DeleteBlob(filePath).GetAwaiter().GetResult();
            }

            //Assertions
            Assert.True(3 == blobsWithPrefix.Count);
            Assert.Equal("listtestfile1", blobsWithPrefix[0].FileName);
            Assert.Equal("listtestfile10", blobsWithPrefix[1].FileName);
            Assert.Equal("listtestfile11", blobsWithPrefix[2].FileName);
        }

        [Fact]
        /// <summary>Verifies blob metadata can be updated</summary>
        public void Test_BlobStorage_UpdateBlobMetadata()
        {
            TearUpDown(async (size, name) =>
            {
                var blob = (BlobItem)_client.GetBlob(_fullPath, true).GetAwaiter().GetResult();

                Assert.True(blob.Metadata.ContainsKey("testKey"));
                Assert.True(blob.Metadata.ContainsValue("testValue"));

                //add more metadata 
                blob.Metadata.AddRange(new Dictionary<string, string> { { "additonalTestKey", "addiionalTestValue" } });

                await _client.UpdateBlobMetadata(blob);

                Assert.True(blob.Metadata.ContainsKey("testKey"));
                Assert.True(blob.Metadata.ContainsValue("testValue"));
                Assert.True(blob.Metadata.ContainsKey("additonalTestKey"));
                Assert.True(blob.Metadata.ContainsValue("addiionalTestValue"));

            });
        }

        /// <summary>Verifies contents of directory copied</summary>
        [Fact]
        public void Test_BlobStorage_CopyDirectory()
        {
            // ARRANGE - setup source and destination folders with blobs for testing.
            var destinationContainerName = "testdestination";
            var metaData = new Dictionary<string, string> { { "testKey", "testValue" } };

            // Delete destination container at start.
            _client.DeleteContainer(destinationContainerName).GetAwaiter().GetResult();
            Task.Delay(30000).GetAwaiter().GetResult();

            try
            {
                // Ensure there are some files in the test folder.
                for (int i = 0; i <= 10; i++)
                {
                    _client.UploadBlob($"{TestContainerName}/sourceTestFile{i}",
                        new MemoryStream(Encoding.Default.GetBytes("This is a test this is a test")),
                        metaData).GetAwaiter().GetResult();
                }
                var totalAtSource = _client.ListBlobs(TestContainerName, true).Count();

                // Create a destination container with some blobs
                for (int i = 0; i < 10; i++)
                {
                    _client.UploadBlob($"{destinationContainerName}/destinationTestFile{i}",
                        new MemoryStream(Encoding.Default.GetBytes("This is a test this is a test"))).GetAwaiter().GetResult();
                }

                var totalAtDestination = _client.ListBlobs(destinationContainerName, true).Count();

                List<string> receivedEvents = new List<string>();
                Action<TransferEventType, ITransferEvent> transferEventAction = (transferEvent, success) =>
                {
                    Console.WriteLine("Received event type: " + transferEvent.ToString());
                    receivedEvents.Add(success.ToString());
                };

                // ACT - carrry out server side copy directory.
                var copyBlobsResult = _client.CopyDirectory(TestContainerName, destinationContainerName, transferEventAction).GetAwaiter().GetResult();

                // ASSERT
                // Ensure correct amount of blobs transferred
                Assert.Equal(totalAtSource, copyBlobsResult.NumberOfFilesTransferred);

                //Ensure the event handler was called for each file transferred
                Assert.Equal(totalAtSource, receivedEvents.Count);

                // Enure all blobs have been copied and existing ones havent been overwritten
                var blobs = _client.ListBlobs(destinationContainerName, true);
                var expectedDesinationCount = copyBlobsResult.NumberOfFilesTransferred + totalAtDestination;
                Assert.Equal(expectedDesinationCount, blobs.Count());

                // Ensure all metadata has been maintained on the copied blobs
                var copiedBlobs = _client.ListBlobs(destinationContainerName, true, true, "sourceTestFile");
                var copiedBlob = (BlobItem)copiedBlobs.First();
                var metadata = copiedBlob.Metadata;
                Assert.True(metadata.ContainsKey("testKey"));
                Assert.True(metadata.ContainsValue("testValue"));
                Assert.True(copiedBlobs.Count() >= 10);
            }
            finally
            {
                // Delete source destination files
                foreach (var file in _client.ListBlobs(TestContainerName, true, false, "sourceTestFile"))
                {
                    _client.DeleteBlob(file.Path).GetAwaiter().GetResult();
                }

                // Delete destination container 
                _client.DeleteContainer(destinationContainerName).GetAwaiter().GetResult();
            }
        }

        [Theory]
        [ClassData(typeof(SignedAccessUrlTestData))]
        public void Test_BlobStorage_GetSignedFolderAccessUrl(Dictionary<string, string> expectedOutputs, ISignedAccessConfig testAccessConfig, string folderPath)
        {
            var accessUrl = _client.GetSignedFolderAccessUrl(folderPath, testAccessConfig).GetAwaiter().GetResult();

            //Assertions
            Assert.NotNull(accessUrl);
            Assert.Contains(expectedOutputs["ExpiryDate"], accessUrl);
            Assert.Contains(expectedOutputs["Permission"], accessUrl);
            Assert.Contains(expectedOutputs["FolderPath"], accessUrl);
        }

        private void TearUpDown(Action<long, string> execute, Action<Exception> exception = null, Action completed = null)
        {
            // Create the file to begin with.
            var fileSize = CreateTestFile(_fullPath).GetAwaiter().GetResult();

            try
            {
                execute(fileSize, TestFileName);
            }
            catch (Exception e)
            {
                if (exception != null)
                    exception.Invoke(e);
                else
                    throw;
            }
            finally
            {
                completed?.Invoke();

                // Remove the file when finished.
                RemoveTestFile(_fullPath);
            }
        }

        private async Task<long> CreateTestFile(string fullPath)
        {
            var metaData = new Dictionary<string, string>
            {
                { "testKey", "testValue" }
            };

            var stringInMemoryStream = new MemoryStream(Encoding.Default.GetBytes("This is a test this is a test"));
            await _client.UploadBlob(fullPath, stringInMemoryStream, metaData);
            return stringInMemoryStream.Length;
        }

        private void RemoveTestFile(string fullPath)
        {
            //_client.AddFolder(TestContainerName).GetAwaiter().GetResult();
            var blob = _client.GetBlob(fullPath).GetAwaiter().GetResult();
            if (blob != null)
            {
                ((BlobItem)blob).UniqueLeaseName = _lease;
                _client.UnlockBlob(blob);
                _client.DeleteBlob(_fullPath).GetAwaiter().GetResult();
            }
        }

        public void Dispose()
        {
            _client.Dispose();
        }
    }
}
