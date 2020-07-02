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
using System.Collections.Generic;
using Cloud.Core.Exceptions;
using Cloud.Core.Storage.AzureBlobStorage.Tests.Fakes;

namespace Cloud.Core.Storage.AzureBlobStorage.Tests.Unit
{
    [IsUnit]
    public class BlobStorageUnitTests
    {
        private const string TESTCONTAINERNAME = "testing";
        private const string TESTFILENAME = "testfile.txt";
        private readonly string _fullPath = $"{TESTCONTAINERNAME}/{TESTFILENAME}";

        [Fact]
        public void Test_ServicePrincipleConfig_ToString()
        {
            // Arrange.
            var config = new ServicePrincipleConfig { };
            var configWithSecret = new ServicePrincipleConfig { AppSecret = "sample" };

            // Act.
            var configNotSet = config.ToString();
            var configSet = configWithSecret.ToString();

            // Assert.
            configNotSet.Contains("Not Set").Should().BeTrue();
            configSet.Contains("Not Set").Should().BeFalse();
        }

        /// <summary>Verify connection config works as expected.</summary>
        [Fact]
        public void Test_ConnectionConfig_InstanceName()
        {
            // Arrange
            var config1 = new ConnectionConfig();
            var config2 = new ConnectionConfig();
            var config3 = new ConnectionConfig();

            // Act
            config1.ConnectionString = "AB";
            config2.ConnectionString = "A;B";
            config3.ConnectionString = "A;AccountName=B;C";

            // Assert
            config1.InstanceName.Should().Be(null);
            config2.InstanceName.Should().Be(null);
            config3.InstanceName.Should().Be("B");
            config1.InstanceName.Should().BeNull();
        }

        /// <summary>Verify service collection add blob storage method adds the blob storage services as expected.</summary>
        [Fact]
        public void Test_ServiceCollection_AddBlobStorageSingleton()
        {
            // Arrange
            IServiceCollection serviceCollection = new FakeServiceCollection();

            // Act/Assert
            serviceCollection.AddBlobStorageSingleton("test", "test", "test");
            serviceCollection.Contains(new ServiceDescriptor(typeof(IBlobStorage), typeof(BlobStorage))).Should().BeTrue();
            serviceCollection.Any(x => x.ServiceType == typeof(NamedInstanceFactory<IBlobStorage>)).Should().BeTrue();
            serviceCollection.Clear();

            // Act/Assert
            serviceCollection.AddBlobStorageSingletonNamed("key1", "test", "test", "test");
            serviceCollection.AddBlobStorageSingletonNamed("key2", "test", "test", "test");
            serviceCollection.AddBlobStorageSingleton("test1", "test", "test");
            serviceCollection.Contains(new ServiceDescriptor(typeof(IBlobStorage), typeof(BlobStorage))).Should().BeTrue();
            serviceCollection.Any(x => x.ServiceType == typeof(NamedInstanceFactory<IBlobStorage>)).Should().BeTrue();

            // Act/Assert
            var prov = serviceCollection.BuildServiceProvider();
            var resolvedFactory = prov.GetService<NamedInstanceFactory<IBlobStorage>>();
            resolvedFactory["key1"].Should().NotBeNull();
            resolvedFactory["key2"].Should().NotBeNull();
            resolvedFactory["test1"].Should().NotBeNull();
            serviceCollection.Clear();

            // Act/Assert
            serviceCollection.AddBlobStorageSingleton(new ServicePrincipleConfig { InstanceName = "test", AppId = "test", AppSecret = "test", TenantId = "test", SubscriptionId = "test" });
            serviceCollection.Contains(new ServiceDescriptor(typeof(IBlobStorage), typeof(BlobStorage))).Should().BeTrue();
            serviceCollection.Clear();

            // Act/Assert
            serviceCollection.AddBlobStorageSingleton(new ConnectionConfig { ConnectionString = "test" });
            serviceCollection.Contains(new ServiceDescriptor(typeof(IBlobStorage), typeof(BlobStorage))).Should().BeTrue();
        }

        /// <summary>Verify the BlobItem created from a cloud block blob is created as expected.</summary>
        [Fact]
        public void Test_BlobItem_FromCloudBlockBlob()
        {
            // Arrange
            var cbItem = new CloudBlockBlob(new Uri("http://www.test.com/samle.txt"));
            var fakeItem = new BlobItem(new FakeBlob());
            var anotherItem = new BlobItem();

            // Act
            var item = new BlobItem(cbItem);

            // Assert
            item.FileName.Should().Be("samle.txt");
            item.Path.Should().Be("$root/samle.txt");
            fakeItem.FileName.Should().Be("myfile.txt");
            anotherItem.Path.Should().BeNullOrEmpty();
        }

        /// <summary>Verify the MsiConfig validates as expected.</summary>
        [Fact]
        public void Test_MsiConfig_ConfigValidation()
        {
            // Arrange
            var msiConfig = new MsiConfig();

            // Act/Assert - Check the msi config validation.
            Assert.Throws<ValidateException>(() => msiConfig.ThrowIfInvalid());
            msiConfig.InstanceName = "test";
            Assert.Throws<ValidateException>(() => msiConfig.ThrowIfInvalid());
            msiConfig.TenantId = "test";
            Assert.Throws<ValidateException>(() => msiConfig.ThrowIfInvalid());
            msiConfig.SubscriptionId = "test";
            AssertExtensions.DoesNotThrow(() => msiConfig.ThrowIfInvalid());
            msiConfig.ToString().Should().NotBeNullOrEmpty();
        }

        /// <summary>Verify the connnection config validates as expected.</summary>
        [Fact]
        public void Test_ConnectionConfig_ConfigValidation()
        {
            // Arrange
            var connectionConfig = new ConnectionConfig();

            // Act/Assert - Check connection string config validation.
            Assert.Throws<ValidateException>(() => connectionConfig.ThrowIfInvalid());
            connectionConfig.ConnectionString = "test";
            AssertExtensions.DoesNotThrow(() => connectionConfig.ThrowIfInvalid());
            connectionConfig.ToString().Should().NotBeNullOrEmpty();
        }

        /// <summary>Verify the ServicePrinciple config validates as expected.</summary>
        [Fact]
        public void Test_ServicePrincipleConfig_ConfigValidation()
        {
            // Arrange
            var spConfig = new ServicePrincipleConfig();

            // Act/Assert - Check the service Principle config validation.
            Assert.Throws<ValidateException>(() => spConfig.ThrowIfInvalid());
            spConfig.InstanceName = "test";
            Assert.Throws<ValidateException>(() => spConfig.ThrowIfInvalid());
            spConfig.AppId = "test";
            Assert.Throws<ValidateException>(() => spConfig.ThrowIfInvalid());
            spConfig.AppSecret = "test";
            Assert.Throws<ValidateException>(() => spConfig.ThrowIfInvalid());
            spConfig.TenantId = "test";
            Assert.Throws<ValidateException>(() => spConfig.ThrowIfInvalid());
            spConfig.SubscriptionId = "test";
            AssertExtensions.DoesNotThrow(() => spConfig.ThrowIfInvalid());
            spConfig.ToString().Should().NotBeNullOrEmpty();
        }

        /// <summary>Ensure invalid setup for connection config throws an exception.</summary>
        [Fact]
        public void Test_BlobStorage_ClientSetupConnectionString_ArgumentFail()
        {
            // Act/Assert
            Assert.Throws<ValidateException>(() => new BlobStorage(new ConnectionConfig()));
        }

        /// <summary>Ensure invalid setup for connection config throws an exception when gathering a blob.</summary>
        [Fact]
        public void Test_BlobStorage_ClientSetupConnectionString_OperationFail()
        {
            // Arrange
            var client = new BlobStorage(new ConnectionConfig() { ConnectionString = "thisdoesntexist" });

            // Act/Assert
            Assert.Throws<InvalidOperationException>(() => client.GetBlob(_fullPath).GetAwaiter().GetResult());
        }

        /// <summary>Ensure correct connection string config does not error.</summary>
        [Fact]
        public void Test_BlobStorage_ClientSetupConnectionString()
        {
            // Arrange
            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();

            // Act/Assert
            AssertExtensions.DoesNotThrow(() => new BlobStorage(new ConnectionConfig("ConnectionString")));
        }

        /// <summary>Ensure container name is correct for passed in path.</summary>
        /// <param name="expected">The expected.</param>
        /// <param name="testPath">The test path.</param>
        [Theory]
        [InlineData("Container1", "Container1/SubPath1/SubPath2/SubPath3/TestBlob.txt")]
        [InlineData("Container1", "Container1/SubPath1/SubPath2/SubPath3")]
        [InlineData("Container1", "Container1/TestBlob.txt")]
        [InlineData("Container1", "Container1")]
        public void Test_BlobStorage_GetContainerName(string expected, string testPath)
        {
            // Arrange
            var blobStorage = new BlobStorage(new ConnectionConfig("none"));

            // Act
            var foundContainerName = blobStorage.GetContainerFromPath(testPath);

            // Assert
            expected.Should().Be(foundContainerName);
        }

        /// <summary>Ensure path without container is formed as expected.</summary>
        /// <param name="expected">The expected.</param>
        /// <param name="testPath">The test path.</param>
        [Theory]
        [InlineData("SubPath1/SubPath2/SubPath3/TestBlob.txt", "Container1/SubPath1/SubPath2/SubPath3/TestBlob.txt")]
        [InlineData("SubPath1/SubPath2/SubPath3", "Container1/SubPath1/SubPath2/SubPath3")]
        [InlineData("SubPath1", "Container1/SubPath1")]
        [InlineData("", "Container1")]
        public void Test_BlobStorage_GetPathWithoutContainer(string expected, string testPath)
        {
            // Arrange
            var blobStorage = new BlobStorage(new ConnectionConfig("none"));

            // Act
            var foundPath = blobStorage.GetPathWithoutContainer(testPath);

            // Assert
            expected.Should().Be(foundPath);
        }

        /// <summary>Ensure the blob relative path is setup as expected.</summary>
        /// <param name="expected">The expected.</param>
        /// <param name="testPath">The test path.</param>
        [Theory]
        [InlineData("SubPath1/SubPath2/SubPath3/TestBlob.txt", "Container1/SubPath1/SubPath2/SubPath3/TestBlob.txt")]
        [InlineData("SubPath1/SubPath2/TestBlob.txt", "Container1/SubPath1/SubPath2/TestBlob.txt")]
        [InlineData("TestBlob.txt", "Container1/TestBlob.txt")]
        [InlineData("TestBlob.txt", "TestBlob.txt")]
        public void Test_BlobStorage_GetBlobRelativePath(string expected, string testPath)
        {
            // Arrange
            var blobStorage = new BlobStorage(new ConnectionConfig("none"));

            // Act
            var foundBlobName = blobStorage.GetBlobRelativePath(testPath);

            // Assert
            expected.Should().Be(foundBlobName);
        }

        /// <summary>Ensure the blob permissions policy is setup as expected.</summary>
        /// <param name="expectedPermissions">The expected permissions.</param>
        /// <param name="testPermissions">The test permissions.</param>
        [Theory]
        [ClassData(typeof(PermissionConversionTestData))]
        public void Test_BlobStorage_GetAzureBlobPolicyPermissions(SharedAccessBlobPermissions expectedPermissions, List<AccessPermission> testPermissions)
        {
            // Arrange
            var blobStorage = new BlobStorage(new ConnectionConfig("none"));

            // Act
            var requestedPermissions = blobStorage.GetAzureBlobPolicyPermissions(testPermissions);

            // Assert
            Assert.Equal(expectedPermissions, requestedPermissions);
        }

        private class FakeBlob : IListBlobItem
        {
            public Uri Uri => new Uri("http://www.test.com/myfile.txt");

            public StorageUri StorageUri => new StorageUri(new Uri("http://www.test.com/myfile.txt"));

            public CloudBlobDirectory Parent => null;

            public CloudBlobContainer Container => new CloudBlobContainer(new Uri("http://www.test.com/myfile.txt"));
        }
    }
}
