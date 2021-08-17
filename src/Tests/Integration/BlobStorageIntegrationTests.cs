using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Cloud.Core.Extensions;
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
        private readonly ConnectionConfig _config;
        private readonly ILogger _logger;
        private const string TestContainerName = "testing";
        private const string TestFileName = "testfile.txt";
        private readonly string _lease = "751C2444-F2C4-484D-8E88-B06FBDF8B4B8";
        private readonly string _fullPath = $"{TestContainerName}/sub1/{TestFileName}";

        public BlobStorageIntegrationTests()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            _config = new ConnectionConfig {
                CreateFolderIfNotExists=true,
                ConnectionString = "DefaultEndpointsProtocol=https;AccountName=cloud1storage;AccountKey=BTrkrpFYX9XVfyKGc19Gi488nfDnpg/H05mZJFIzHWT5hApBZvV4+LUNDE/B2riI558J2sHPcHQHeRBCvtcP8A==;EndpointSuffix=core.windows.net"
            };
            //{
            //    InstanceName = config.GetValue<string>("InstanceName"),
            //    TenantId = config.GetValue<string>("TenantId"),
            //    SubscriptionId = config.GetValue<string>("SubscriptionId"),
            //    AppId = config.GetValue<string>("AppId"),
            //    AppSecret = config.GetValue<string>("AppSecret"),
            //    LockInSeconds = 60,
            //    CreateFolderIfNotExists = true
            //};

            _logger = new ServiceCollection().AddLogging(builder => builder.AddConsole())
                .BuildServiceProvider().GetService<ILogger<BlobStorageIntegrationTests>>();

            _client = new BlobStorage(_config, _logger);
            RemoveTestFile(_fullPath);
        }

        /// <summary>Verify the cached config returns from cache, rather than rebuilding the connection string each time.</summary>
        [Fact]
        public void Test_BlobStorage_CachedConnectionString()
        {
            // Arrange
            var targetDir = Environment.CurrentDirectory + "\\test.txt";
            TearUpDown((size, name) =>
            {
                _client.DownloadBlob(_fullPath, targetDir).GetAwaiter().GetResult();

                Assert.True(File.Exists(targetDir));

                _client.ConnectionString.Should().NotBeNullOrEmpty();

            });
            var testInstance = new BlobStorage(_config);

            // Act
            var internalClient = testInstance.CloudBlobClient; // initialise the client connection.

            // Assert
            testInstance.ConnectionString.Should().NotBeNullOrEmpty();
        }

        /// <summary>Defines the test method for checking msi authentication raises erros when not set correctly.</summary>
        [Fact]
        public void Test_BlobStorage_MsiConfig()
        {
            // Arrange
            var client = new BlobStorage(new MsiConfig { InstanceName = "test", SubscriptionId = "test", TenantId = "test" });

            // Act/Assert
            Assert.Throws<InvalidOperationException>(() => client.BuildStorageConnection().GetAwaiter().GetResult());
        }

        /// <summary>Verifies getting a blob that does not exist returns the correct result.</summary>
        [Fact]
        public void Test_BlobStorage_GetBlobExistsAndNotExists()
        {
            // Arrange
            TearUpDown((size, name) =>
            {

                // Get the blob with a lock.
                var blobExists = _client.GetBlob(_fullPath).GetAwaiter().GetResult();
                blobExists.Should().NotBeNull();

                // Act
                var blobDoesNotExist = _client.GetBlob($"{TestContainerName}/DOESNOTEXIST").GetAwaiter().GetResult();

                // Assert
                blobDoesNotExist.Should().BeNull();
            });
        }

        /// <summary>Verifies locks can be applied and removed on blob items.</summary>
        [Fact]
        public void Test_BlobStorage_GetBlobLockUnlock()
        {
            // Arrange
            TearUpDown((size, name) =>
            {
                // Get the blob with a lock.
                var blob = _client.GetBlobWithLock(_fullPath, _lease).GetAwaiter().GetResult();
                Thread.Sleep(15);
                _client.UnlockBlob(blob);

                // Act
                var content = _client.DownloadBlob(blob).GetAwaiter().GetResult();

                // Assert
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
            // Assert - Test a file can be created and removed without exception.
            AssertExtensions.DoesNotThrow(() =>
            {
                // Arrange/Act
                TearUpDown((size, name) => { });
            });
        }

        /// <summary>Verifies a file can be downloaded from storage and saved to disk.</summary>
        [Fact]
        public void Test_BlobStorage_DownloadFileToDisk()
        {
            // Arrange
            TearUpDown((size, name) =>
            {
                var targetDir = Environment.CurrentDirectory + "\\test.txt";

                // Act
                _client.DownloadBlob(_fullPath, targetDir).GetAwaiter().GetResult();

                // Assert
                Assert.True(File.Exists(targetDir));
            });
        }

        /// <summary>Verifies a file can be downloaded into a memory stream.</summary>
        [Fact]
        public void Test_BlobStorage_DownloadFileStream()
        {
            // Arrange
            TearUpDown((size, name) =>
            {
                // Act
                var blob = _client.GetBlob(_fullPath, true).GetAwaiter().GetResult();

                string text;
                long fileSize;
                using (var stream = _client.DownloadBlob(blob).GetAwaiter().GetResult())
                {
                    Assert.NotNull(stream);
                    fileSize = stream.Length;
                    Assert.Equal(size, fileSize);

                    using var reader = new StreamReader(stream);
                    text = reader.ReadToEnd();
                    Assert.Equal("This is a test this is a test", text);
                }

                var targetDir = Environment.CurrentDirectory + "\\test.txt";
                _client.DownloadBlob(_fullPath, targetDir).GetAwaiter().GetResult();

                // Assert
                Assert.True(File.Exists(targetDir));
            });
        }

        /// <summary>Verifies base path can be set for efficiency.</summary>
        [Fact]
        public void Test_BlobStorage_SetBasePath()
        {
            // Arrange
            _client.BasePath = TestContainerName;
            TearUpDown((size, name) =>
            {
                // Act/Assert
                _client.Exists(TestFileName).GetAwaiter().GetResult().Should().BeTrue();
            });

            // Act/Assert
            _client.Exists(TestFileName).GetAwaiter().GetResult().Should().BeFalse();
        }

        /// <summary>Confirms blob exists returns the correct result.</summary>
        [Fact]
        public void Test_BlobStorage_BlobExists()
        {
            // Arrange
            TearUpDown((size, name) =>
            {
                // Act/Assert
                _client.Exists(_fullPath).GetAwaiter().GetResult().Should().BeTrue();
            });

            // Act/Assert
            _client.Exists(_fullPath).GetAwaiter().GetResult().Should().BeFalse();
        }

        /// <summary>Verifies upload followed by download to disk works as expected when called together.</summary>
        [Fact]
        public void Test_BlobStorage_UploadxDownloadToDisk()
        {
            // Arrange
            var localFilePath = $"{AppDomain.CurrentDomain.BaseDirectory}\\{TestFileName}";

            // Act
            File.WriteAllText(localFilePath, Lorem.GetParagraph());
            _client.UploadBlob(_fullPath, localFilePath).GetAwaiter().GetResult();
            File.Delete(localFilePath);
            _client.DownloadBlob(_fullPath, localFilePath).GetAwaiter().GetResult();

            // Assert
            Assert.True(File.Exists(localFilePath));
            File.Delete(localFilePath);
        }

        /// <summary>Verifies listing the files works as expected.</summary>
        [Fact]
        public void Test_BlobStorage_ListFiles()
        {
            // Arrange
            TearUpDown((size, name) =>
            {
                // Act
                var files = _client.ListBlobs(TestContainerName, false);

                // Assert
                Assert.True(files.Select(f => f.FileName == TestFileName).Any());
            });
        }

        /// <summary>Verifies listing the folders (containers) works as expected.</summary>
        [Fact]
        public void Test_BlobStorage_ListFolders()
        {
            // Arrange
            TearUpDown((size, name) =>
            {
                // Act
                var folders = _client.ListFolders().GetAwaiter().GetResult();

                // Assert
                Assert.True(folders.Any());
            });
        }

        /// <summary>Verifies a file with attributes can be downloaded and attributes read.</summary>
        [Fact]
        public void Test_BlobStorage_GetBlobWithAttributes()
        {
            // Act
            TearUpDown((size, name) =>
            {
                var localFilePath = $"{AppDomain.CurrentDomain.BaseDirectory}\\{TestFileName}";

                // Act
                var blob = (BlobItem)_client.GetBlob(_fullPath, true).GetAwaiter().GetResult();
                var content = _client.DownloadBlob(blob).GetAwaiter().GetResult();
                File.WriteAllBytes(localFilePath, ((MemoryStream)content).GetBuffer());

                // Assert
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
            // Arrange
            TearUpDown((size, name) =>
            {
                // Act
                _client.ListBlobsObservable(TestContainerName, true, true).Take(1).Subscribe(blobItem =>
                {
                    // Assert
                    Assert.True(blobItem.FileName == TestFileName);
                });
            });
        }

        /// <summary>Verifies content hash of file is as exepected.</summary>
        [Fact]
        public void Test_BlobStorage_GetBlobWithAttributesParsesContentHashCorrectlyAsync()
        {
            // Arrange
            TearUpDown((size, name) =>
            {
                var rg = new Regex(@"^[a-zA-Z0-9\s,]*$");

                // Act
                var azureBlob = _client.GetBlockBlobReference(_fullPath);
                azureBlob.FetchAttributesAsync().GetAwaiter().GetResult();
                var azureContentMd5 = Convert.FromBase64String(azureBlob.Properties.ContentMD5);
                var decodedAzureContentHash = BitConverter.ToString(azureContentMd5).Replace("-", string.Empty).ToUpper();

                var coreBlob = _client.GetBlob(_fullPath, true).GetAwaiter().GetResult();

                // Assert
                Assert.True(decodedAzureContentHash == coreBlob.ContentHash);
                Assert.True(coreBlob.ContentHash.IsEquivalentTo(coreBlob.ContentHash.ToUpper()));
                Assert.Matches(rg, coreBlob.ContentHash);
            });
        }

        /// <summary>Verifies blobs can be listed based off a prefix.</summary>
        [Fact]
        public void Test_BlobStorage_ListFilesWithPrefix()
        {
            // Arrange
            var metaData = new Dictionary<string, string>
            {
                { "testKey", "testValue" }
            };

            // Act - Create blobs
            for (int i = 1; i <= 11; i++)
            {
                string filePath = $"{TestContainerName}/listtestfile{i}";

                var stringInMemoryStream = new MemoryStream(Encoding.Default.GetBytes("This is a test this is a test"));
                _client.UploadBlob(filePath, stringInMemoryStream, metaData).GetAwaiter().GetResult();
            }

            // Get Blobs
            var blobsWithPrefix = _client.ListBlobs(TestContainerName, false, false, "listtestfile1").ToList();

            // Cleanup Blobs
            for (int i = 1; i <= 11; i++)
            {
                string filePath = $"{TestContainerName}/listtestfile{i}";

                var blob = _client.GetBlob(filePath).GetAwaiter().GetResult();
                ((BlobItem)blob).UniqueLeaseName = _lease;
                _client.UnlockBlob(blob);
                _client.DeleteBlob(filePath).GetAwaiter().GetResult();
            }

            // Assert
            Assert.True(3 == blobsWithPrefix.Count);
            Assert.Equal("listtestfile1", blobsWithPrefix[0].FileName);
            Assert.Equal("listtestfile10", blobsWithPrefix[1].FileName);
            Assert.Equal("listtestfile11", blobsWithPrefix[2].FileName);
        }

        [Fact]
        /// <summary>Verifies blob metadata can be updated as expected.</summary>
        public void Test_BlobStorage_UpdateBlobMetadata()
        {
            // Arrange
            TearUpDown(async (size, name) =>
            {
                var blob = (BlobItem)_client.GetBlob(_fullPath, true).GetAwaiter().GetResult();

                Assert.True(blob.Metadata.ContainsKey("testKey"));
                Assert.True(blob.Metadata.ContainsValue("testValue"));

                // Act - add more metadata 
                blob.Metadata.AddRange(new Dictionary<string, string> { { "additonalTestKey", "addiionalTestValue" } });

                await _client.UpdateBlobMetadata(blob);

                // Assert
                Assert.True(blob.Metadata.ContainsKey("testKey"));
                Assert.True(blob.Metadata.ContainsValue("testValue"));
                Assert.True(blob.Metadata.ContainsKey("additonalTestKey"));
                Assert.True(blob.Metadata.ContainsValue("addiionalTestValue"));

            });
        }

        /// <summary>Verifies contents of directory copied across as expected.</summary>
        [Fact]
        public void Test_BlobStorage_CopyDirectory()
        {
            // Arrange - setup source and destination folders with blobs for testing.
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

                    Console.WriteLine($"Starttime: {success.StartTime}");
                    Console.WriteLine($"Source: {success.Source}");
                    Console.WriteLine($"Destination: {success.Destination}");
                    Console.WriteLine($"Endtime: {success.EndTime}");
                };

                // Act - carrry out server side copy directory.
                var copyBlobsResult = _client.CopyDirectory(TestContainerName, destinationContainerName, transferEventAction).GetAwaiter().GetResult();

                Console.WriteLine($"totalAtSource: {totalAtSource}, NumberOfFilesTransferred: {copyBlobsResult.NumberOfFilesTransferred}");

                // Assert - Ensure correct amount of blobs transferred
                Assert.Equal(totalAtSource, copyBlobsResult.NumberOfFilesTransferred);
                Assert.True(copyBlobsResult.NumberOfFilesFailed == 0);
                Assert.True(copyBlobsResult.NumberOfFilesSkipped == 0);
                Assert.True(copyBlobsResult.BytesTransferred > 0);

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
                // Clean-up
                // Delete source destination files
                foreach (var file in _client.ListBlobs(TestContainerName, true, false, "sourceTestFile"))
                {
                    _client.DeleteBlob(file.Path).GetAwaiter().GetResult();
                }

                // Delete destination container 
                _client.DeleteContainer(destinationContainerName).GetAwaiter().GetResult();
            }
        }

        /// <summary>Verify copying a single file works as expected.</summary>
        [Fact]
        public async void Test_BlobStorage_CopyFile()
        {
            // Arrange.
            var destPath = $"other/{TestFileName}";

            // Act
            // Ensure files are deleted to begin with.
            await _client.DeleteBlob(_fullPath);
            await _client.DeleteBlob(destPath);

            // Check before and after file copy.
            var existsBefore = await _client.Exists(destPath);
            await CreateTestFile(_fullPath);
            await _client.CopyFile(_fullPath, destPath);
            var existsAfter = await _client.Exists(destPath);

            // Assert.
            existsBefore.Should().BeFalse();
            existsAfter.Should().BeTrue();
        }

        /// <summary>Verify we can upload a stream efficiently.</summary>
        [Fact]
        public async void Test_BlobStorage_UploadStreamAsync()
        {
            // Arrange.
            var destPath = $"other/someBlobName.csv";

            // Act
            // Check file doesn't exist before.
            await _client.DeleteBlob(destPath);
            var existsBefore = await _client.Exists(destPath);

            // Upload stream in parts.
            using (var str = await _client.UploadBlobFromStream(destPath))
            {
                using var csvFile = new StreamWriter(str);
                for (int i = 1; i <= 100; ++i)
                {
                    string rec = string.Format("{0}, a{0}, b{0}, c{0}, d{0}", i);
                    csvFile.WriteLine(rec);
                }

                csvFile.Flush();
            }

            // Check file exists after.
            var existsAfter = await _client.Exists(destPath);
            await _client.DeleteBlob(destPath);

            // Assert.
            existsBefore.Should().BeFalse();
            existsAfter.Should().BeTrue();
        }

        /// <summary>Verify we can upload a stream efficiently.</summary>
        [Fact]
        public async void Test_BlobStorage_DownloadStreamAsync()
        {
            // Arrange.
            var destPath = $"other/someBlobName.csv";

            // Act
            // Upload file to begin with.
            using (var str = await _client.UploadBlobFromStream(destPath))
            {
                using var csvFile = new StreamWriter(str);
                for (int i = 1; i <= 100; ++i)
                {
                    string rec = string.Format("{0}, a{0}, b{0}, c{0}, d{0}", i);
                    csvFile.WriteLine(rec);
                }

                csvFile.Flush();
            }

            int lines = 0;

            // Download the file in parts.
            using (var file = await _client.DownloadBlobToStream(destPath))
            {
                using var sr = new StreamReader(file, Encoding.UTF8);

                do
                {
                    var line = await sr.ReadLineAsync();
                    var compare = string.Format("{0}, a{0}, b{0}, c{0}, d{0}", lines + 1);
                    line.Should().BeEquivalentTo(compare);
                    lines++;
                } while (!sr.EndOfStream);

            }

            // Assert.
            lines.Should().Be(100);
        }

        /// <summary>Verify the call to signed folder access url returns the correct url as expected.</summary>
        /// <param name="expectedOutputs">The expected outputs.</param>
        /// <param name="testAccessConfig">The test access configuration.</param>
        /// <param name="folderPath">The folder path.</param>
        [Theory]
        [ClassData(typeof(SignedAccessUrlTestData))]
        public void Test_BlobStorage_GetSignedFolderAccessUrl(Dictionary<string, string> expectedOutputs, ISignedAccessConfig testAccessConfig, string folderPath)
        {
            // Arrange/Act
            var accessUrl = _client.GetSignedFolderAccessUrl(folderPath, testAccessConfig).GetAwaiter().GetResult();

            // Assert
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
