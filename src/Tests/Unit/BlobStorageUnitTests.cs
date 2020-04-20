using System;
using System.Linq;
using Cloud.Core.Storage.AzureBlobStorage.Config;
using Cloud.Core.Testing;
using FluentAssertions;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Cloud.Core.Storage.AzureBlobStorage.Tests.Unit
{
    using System.Collections.Generic;

    [IsUnit]
    public class BlobStorageUnitTests
    {
        private const string TESTCONTAINERNAME = "testing";
        private const string TESTFILENAME = "testfile.txt";
        private readonly string _fullPath = $"{TESTCONTAINERNAME}/{TESTFILENAME}";

        private class FakeBlob : IListBlobItem
        {
            public Uri Uri => new Uri("http://www.test.com/myfile.txt");

            public StorageUri StorageUri => new StorageUri(new Uri("http://www.test.com/myfile.txt"));

            public CloudBlobDirectory Parent => null;

            public CloudBlobContainer Container => new CloudBlobContainer(new Uri("http://www.test.com/myfile.txt"));
        }

        public BlobStorageUnitTests()
        {
        }

        [Fact]
        public void Test_ConnectionConfig_InstanceName()
        {
            var config = new ConnectionConfig();
            config.InstanceName.Should().BeNull();

            config.ConnectionString = "AB";
            config.InstanceName.Should().Be(null);

            config.ConnectionString = "A;B";
            config.InstanceName.Should().Be(null);

            config.ConnectionString = "A;AccountName=B;C";
            config.InstanceName.Should().Be("B");
        }

        [Fact]
        public void Test_BlobStorage_ServiceCollectionAddKeyVault()
        {
            IServiceCollection serviceCollection = new FakeServiceCollection();

            serviceCollection.AddBlobStorageSingleton("test", "test", "test");
            serviceCollection.Contains(new ServiceDescriptor(typeof(IBlobStorage), typeof(BlobStorage))).Should().BeTrue();
            serviceCollection.Any(x => x.ServiceType == typeof(NamedInstanceFactory<IBlobStorage>)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddBlobStorageSingletonNamed("key1", "test", "test", "test");
            serviceCollection.AddBlobStorageSingletonNamed("key2", "test", "test", "test");
            serviceCollection.AddBlobStorageSingleton("test1", "test", "test");
            serviceCollection.Contains(new ServiceDescriptor(typeof(IBlobStorage), typeof(BlobStorage))).Should().BeTrue();
            serviceCollection.Any(x => x.ServiceType == typeof(NamedInstanceFactory<IBlobStorage>)).Should().BeTrue();

            var prov = serviceCollection.BuildServiceProvider();
            var resolvedFactory = prov.GetService<NamedInstanceFactory<IBlobStorage>>();
            resolvedFactory["key1"].Should().NotBeNull();
            resolvedFactory["key2"].Should().NotBeNull();
            resolvedFactory["test1"].Should().NotBeNull();
            serviceCollection.Clear();

            serviceCollection.AddBlobStorageSingleton(new ServicePrincipleConfig { InstanceName = "test", AppId = "test", AppSecret = "test", TenantId = "test", SubscriptionId = "test" });
            serviceCollection.Contains(new ServiceDescriptor(typeof(IBlobStorage), typeof(BlobStorage))).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddBlobStorageSingleton(new ConnectionConfig { ConnectionString = "test" });
            serviceCollection.Contains(new ServiceDescriptor(typeof(IBlobStorage), typeof(BlobStorage))).Should().BeTrue();
        }

        [Fact]
        public void Test_BlobStorage_BlobItem()
        {
            var cbItem = new CloudBlockBlob(new Uri("http://www.test.com/samle.txt"));
            var item = new BlobItem(cbItem);

            item.FileName.Should().Be("samle.txt");
            item.Path.Should().Be("$root/samle.txt");

            var fakeItem = new BlobItem(new FakeBlob());
            fakeItem.FileName.Should().Be("myfile.txt");

            var anotherItem = new BlobItem();
            anotherItem.Path.Should().BeNullOrEmpty();
        }

        [Fact]
        public void Test_BlobStorage_ConfigValidation()
        {
            var msiConfig = new MsiConfig();
            var connectionConfig = new ConnectionConfig();
            var spConfig = new ServicePrincipleConfig();

            // Check the msi config validation.
            Assert.Throws<ArgumentException>(() => msiConfig.Validate());
            msiConfig.InstanceName = "test";
            Assert.Throws<ArgumentException>(() => msiConfig.Validate());
            msiConfig.TenantId = "test";
            Assert.Throws<ArgumentException>(() => msiConfig.Validate());
            msiConfig.SubscriptionId = "test";
            AssertExtensions.DoesNotThrow(() => msiConfig.Validate());
            msiConfig.ToString().Should().NotBeNullOrEmpty();

            // Check connection string config validation.
            Assert.Throws<ArgumentException>(() => connectionConfig.Validate());
            connectionConfig.ConnectionString = "test";
            AssertExtensions.DoesNotThrow(() => connectionConfig.Validate());
            connectionConfig.ToString().Should().NotBeNullOrEmpty();

            // Check the service Principle config validation.
            Assert.Throws<ArgumentException>(() => spConfig.Validate());
            spConfig.InstanceName = "test";
            Assert.Throws<ArgumentException>(() => spConfig.Validate());
            spConfig.AppId = "test";
            Assert.Throws<ArgumentException>(() => spConfig.Validate());
            spConfig.AppSecret = "test";
            Assert.Throws<ArgumentException>(() => spConfig.Validate());
            spConfig.TenantId = "test";
            Assert.Throws<ArgumentException>(() => spConfig.Validate());
            spConfig.SubscriptionId = "test";
            AssertExtensions.DoesNotThrow(() => spConfig.Validate());
            spConfig.ToString().Should().NotBeNullOrEmpty();
        }

        [Fact]
        public void Test_BlobStorage_ClientSetupConnectionString_ArgumentFail()
        {
            Assert.Throws<ArgumentException>(() => new BlobStorage(new ConnectionConfig()));
        }

        [Fact]
        public void Test_BlobStorage_ClientSetupConnectionString_OperationFail()
        {
            var client = new BlobStorage(new ConnectionConfig() { ConnectionString = "thisdoesntexist" });
            Assert.Throws<InvalidOperationException>(() => client.GetBlob(_fullPath).GetAwaiter().GetResult());
        }

        [Fact]
        public void Test_BlobStorage_ClientSetupConnectionString()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            AssertExtensions.DoesNotThrow(() => new BlobStorage(new ConnectionConfig("ConnectionString")));
        }

        [Theory]
        [InlineData("Container1", "Container1/SubPath1/SubPath2/SubPath3/TestBlob.txt")]
        [InlineData("Container1", "Container1/SubPath1/SubPath2/SubPath3")]
        [InlineData("Container1", "Container1/TestBlob.txt")]
        [InlineData("Container1", "Container1")]
        public void Test_BlobStorage_GetContainerName(string expected, string testPath)
        {
            var blobStorage = new BlobStorage(new ConnectionConfig("none"));
            var foundContainerName = blobStorage.GetContainerFromPath(testPath);
            expected.Should().Be(foundContainerName);
        }

        [Theory]
        [InlineData("SubPath1/SubPath2/SubPath3/TestBlob.txt", "Container1/SubPath1/SubPath2/SubPath3/TestBlob.txt")]
        [InlineData("SubPath1/SubPath2/SubPath3", "Container1/SubPath1/SubPath2/SubPath3")]
        [InlineData("SubPath1", "Container1/SubPath1")]
        [InlineData("", "Container1")]
        public void Test_BlobStorage_GetPathWithoutContainer(string expected, string testPath)
        {
            var blobStorage = new BlobStorage(new ConnectionConfig("none"));
            var foundPath = blobStorage.GetPathWithoutContainer(testPath);
            expected.Should().Be(foundPath);
        }

        [Theory]
        [InlineData("SubPath1/SubPath2/SubPath3/TestBlob.txt", "Container1/SubPath1/SubPath2/SubPath3/TestBlob.txt")]
        [InlineData("SubPath1/SubPath2/TestBlob.txt", "Container1/SubPath1/SubPath2/TestBlob.txt")]
        [InlineData("TestBlob.txt", "Container1/TestBlob.txt")]
        [InlineData("TestBlob.txt", "TestBlob.txt")]
        public void Test_BlobStorage_GetBlobRelativePath(string expected, string testPath)
        {
            var blobStorage = new BlobStorage(new ConnectionConfig("none"));
            var foundBlobName = blobStorage.GetBlobRelativePath(testPath);
            expected.Should().Be(foundBlobName);
        }

        [Theory]
        [ClassData(typeof(PermissionConversionTestData))]
        public void Test_BlobStorage_GetAzureBlobPolicyPermissions(SharedAccessBlobPermissions expectedPermissions, List<AccessPermission> testPermissions)
        {
            var blobStorage = new BlobStorage(new ConnectionConfig("none"));
            var requestedPermissions = blobStorage.GetAzureBlobPolicyPermissions(testPermissions);
            Assert.Equal(expectedPermissions, requestedPermissions);
        }
    }
}
