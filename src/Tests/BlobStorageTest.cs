using System;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cloud.Core.Storage.AzureBlobStorage.Config;
using Cloud.Core.Testing;
using Cloud.Core.Testing.Lorem;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Cloud.Core.Storage.AzureBlobStorage.Tests
{
    public class BlobStorageTest : IDisposable
    {
        private readonly BlobStorage _client;
        private const string TESTCONTAINERNAME = "testing";
        private const string TESTFILENAME = "testfile.txt";
        private string _lease = "751C2444-F2C4-484D-8E88-B06FBDF8B4B8";
        private readonly string _fullPath = $"{TESTCONTAINERNAME}/{TESTFILENAME}";

        public BlobStorageTest()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            var blobConfig = new ServicePrincipleConfig()
            {
                LockInSeconds = 15,
                StorageInstanceName = config.GetValue<string>("StorageInstanceName"),
                AppSecret = config.GetValue<string>("AppSecret"),
                TenantId = config.GetValue<string>("TenantId"),
                AppId = config.GetValue<string>("AppId"),
                SubscriptionId = config.GetValue<string>("SubscriptionId"),
            };
            
            _client = new BlobStorage(blobConfig);

            config.ToString();

            RemoveTestFile();
        }

        [Fact, IsIntegration]
        public void Test_BlobStorage_ClientSetupSettings_ArgumentFail()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            var blobConfig = new ServicePrincipleConfig
            {
                LockInSeconds = 20,
                StorageInstanceName = config.GetValue<string>("StorageInstanceName"),
                AppSecret = config.GetValue<string>("AppSecret"),
                TenantId = config.GetValue<string>("TenantId"),
                AppId = config.GetValue<string>("AppId"),
                SubscriptionId = string.Empty,
            };
            var client = new BlobStorage(blobConfig);
            Assert.Throws<Exception>(() => client.GetBlob(_fullPath).GetAwaiter().GetResult());

            client = new BlobStorage(new ServicePrincipleConfig());
            Assert.Throws<Exception>(() => client.GetBlob(_fullPath).GetAwaiter().GetResult());
        }

        [Fact, IsIntegration]
        public void Test_BlobStorage_ClientSetupConnectionString_ArgumentFail()
        {
            Assert.Throws<ArgumentException>(() => new BlobStorage(new ConnectionConfig()));
        }

        [Fact, IsIntegration]
        public void Test_BlobStorage_ClientSetupConnectionString_OperationFail()
        {
            var client = new BlobStorage(new ConnectionConfig() { ConnectionString = "thisdoesntexist" });
            Assert.Throws<InvalidOperationException>(() => client.GetBlob(_fullPath).GetAwaiter().GetResult());
        }

        [Fact, IsIntegration]
        public void Test_BlobStorage_ClientSetupConnectionString()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            AssertExtensions.DoesNotThrow(() => new BlobStorage(new ConnectionConfig
                {ConnectionString = config.GetValue<string>("ConnectionString")}));
        }

        [Fact, IsIntegration]
        public void Test_BlobStorage_ClientSetupMsi()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();



            AssertExtensions.DoesNotThrow(() =>
            {
                var bs = new BlobStorage(new MsiConfig {
                    StorageInstanceName = config.GetValue<string>("StorageInstanceName"),
                    TenantId = config.GetValue<string>("TenantId"), 
                    SubscriptionId = config.GetValue<string>("SubscriptionId")
                });

                var item = bs.GetBlob("testing/testfile.txt").GetAwaiter().GetResult();
                item.FileName.Should().Be("testfile.txt");
            });
        }

        [Fact, IsIntegration]
        public void Test_BlobStorage_GetBlobLockUnlock()
        {
            TearUpDown((size, name) =>
            {
                // Get the blob with a lock.
                IBlobItem blob = _client.GetBlobWithLock(_fullPath, _lease).GetAwaiter().GetResult();
                Thread.Sleep(15);
                _client.UnlockBlob(blob);
                var content = _client.DownloadBlob(blob).GetAwaiter().GetResult();

                Assert.NotNull(content);
                Assert.True(blob.FileName == TESTFILENAME);
                Assert.True(blob.FileNameWithoutExtension == "testfile");
                Assert.True(blob.FileExtension == ".exe");
                Assert.True(blob.RootFolder == TESTCONTAINERNAME);

            });
        }

        [Fact, IsIntegration]
        public void Test_BlobStorage_UploadFile()
        {
            // Test a file can be created and removed without exception.
            AssertExtensions.DoesNotThrow(() =>
            {
                TearUpDown((size, name) => { });
            });
        }

        [Fact, IsIntegration]
        public void Test_BlobStorage_DownloadFile()
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
            });
        }

        [Fact, IsIntegration]
        public void Test_BlobStorage_UploadDownloadToDisk()
        {
            var localFilePath = $"{AppDomain.CurrentDomain.BaseDirectory}\\{TESTFILENAME}";

            File.WriteAllText(localFilePath, Lorem.GetParagraph());
            _client.UploadBlob(_fullPath, localFilePath).GetAwaiter().GetResult();
            File.Delete(localFilePath);
            _client.DownloadBlob(_fullPath, localFilePath).GetAwaiter().GetResult();
            Assert.True(File.Exists(localFilePath));
            File.Delete(localFilePath);
        }

        [Fact, IsIntegration]
        public void Test_BlobStorage_ListFiles()
        {
            TearUpDown((size, name) =>
            {
                var files = _client.ListBlobs(TESTCONTAINERNAME, false);
                Assert.True(files.Select(f => f.FileName == TESTFILENAME).Any());
            });
        }

        [Fact, IsIntegration]
        public void Test_BlobStorage_ListFolders()
        {
            TearUpDown((size, name) =>
            {
                var folders = _client.ListFolders().GetAwaiter().GetResult();
                Assert.True(folders.Any());
            });
        }

        [Fact, IsIntegration]
        public void Test_BlobStorage_GetBlobWithAttributes()
        {
            TearUpDown((size, name) =>
            {
                var localFilePath = $"{AppDomain.CurrentDomain.BaseDirectory}\\{TESTFILENAME}";

                var blob = _client.GetBlob(_fullPath, true).GetAwaiter().GetResult();

                var content = _client.DownloadBlob(blob).GetAwaiter().GetResult();
                File.WriteAllBytes(localFilePath, ((MemoryStream)content).GetBuffer());
                Assert.Equal(size, blob.FileSize);
                Assert.True(File.Exists(localFilePath));
                Assert.NotNull(content);
                Assert.True(blob.FileName == TESTFILENAME);
                Assert.True(blob.RootFolder == TESTCONTAINERNAME);
                Assert.True(blob.FileExtension == ".txt");

                Assert.Throws<InvalidOperationException>(() =>
                {
                    _client.GetBlob("doesnotexist").GetAwaiter().GetResult();
                });
            });
        }

        [Fact, IsIntegration]
        public void Test_BlobStorage_ListFilesSubscribe()
        {
            TearUpDown((size, name) =>
            {
                _client.ListBlobsObservable(TESTCONTAINERNAME, true).Take(1).Subscribe(blobItem =>
                {
                    Assert.True(blobItem.FileName == TESTFILENAME);
                });
            });
        }

        private void TearUpDown(Action<long, string> execute, Action<Exception> exception = null, Action completed = null)
        {
            // Create the file to begin with.
            var fileSize = CreateTestFile().GetAwaiter().GetResult();

            try
            {
                execute(fileSize, TESTFILENAME);
            }
            catch (Exception e)
            {
                exception?.Invoke(e);
            }
            finally
            {
                completed?.Invoke();

                // Remove the file when finished.
                RemoveTestFile();
            }
        }
        
        private async Task<long> CreateTestFile()
        {
            MemoryStream stringInMemoryStream = new MemoryStream(Encoding.Default.GetBytes("This is a test this is a test"));
            await _client.UploadBlob(_fullPath, stringInMemoryStream);
            return stringInMemoryStream.Length;
        }

        private void RemoveTestFile()
        {
            var blob = _client.GetBlob(_fullPath).GetAwaiter().GetResult();
            ((BlobItem) blob).UniqueLeaseName = _lease;
            _client.UnlockBlob(blob);
            _client.DeleteBlob(_fullPath).GetAwaiter().GetResult();
        }

        public void Dispose()
        {
            _client.Dispose();
        }
    }
}
