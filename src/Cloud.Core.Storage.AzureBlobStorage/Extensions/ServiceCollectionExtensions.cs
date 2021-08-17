namespace Microsoft.Extensions.DependencyInjection
{
    using System;
    using Cloud.Core;
    using Cloud.Core.Extensions;
    using Cloud.Core.Storage.AzureBlobStorage;
    using Cloud.Core.Storage.AzureBlobStorage.Config;

    /// <summary>
    /// Class Service Collection extensions.
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Adds an instance of Azure Blob storage as a singleton, using managed user config to setup.  Requires the instance 
        /// name, TenantId and SubscriptionId to be supplied.
        /// NOTE - will create the container if it doesn't already exist.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="key">The key to use when looking up the instance from the factory.</param>
        /// <param name="instanceName">Name of the blob storage instance to connect to.</param>
        /// <param name="tenantId">Tenant Id the instance lives in.</param>
        /// <param name="subscriptionId">Subscription Id for the tenant.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddBlobStorageSingletonNamed(this IServiceCollection services, string key, string instanceName, string tenantId, string subscriptionId)
        {
            var instance = new BlobStorage(new MsiConfig
            {
                InstanceName = instanceName,
                TenantId = tenantId,
                SubscriptionId = subscriptionId,
                CreateFolderIfNotExists = true
            });

            if (!key.IsNullOrEmpty())
                instance.Name = key;

            services.AddSingleton<IBlobStorage>(instance);
            services.AddFactoryIfNotAdded<IBlobStorage>();
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure Blob storage as a singleton, using managed user config to setup.  Requires the instance 
        /// name, TenantId and SubscriptionId to be supplied.
        /// NOTE - will create the container if it doesn't already exist.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="instanceName">Name of the blob storage instance to connect to.</param>
        /// <param name="tenantId">Tenant Id the instance lives in.</param>
        /// <param name="subscriptionId">Subscription Id for the tenant.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddBlobStorageSingleton(this IServiceCollection services, string instanceName, string tenantId, string subscriptionId)
        {
            services.AddBlobStorageSingleton(new MsiConfig {
                InstanceName = instanceName, TenantId = tenantId, SubscriptionId = subscriptionId,
                CreateFolderIfNotExists = true
            });
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure Blob storage as a singleton, using managed user config to setup.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="config">The configuration to initialise with.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddBlobStorageSingleton(this IServiceCollection services, MsiConfig config)
        {
            services.AddSingleton<IBlobStorage>(new BlobStorage(config));
            services.AddFactoryIfNotAdded<IBlobStorage>();
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure Blob storage as a singleton, using service principle config to setup.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="config">The configuration to initialise with.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddBlobStorageSingleton(this IServiceCollection services, ServicePrincipleConfig config)
        {
            services.AddSingleton<IBlobStorage>(new BlobStorage(config));
            services.AddFactoryIfNotAdded<IBlobStorage>();
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure Blob storage as a singleton, using connection string config to setup.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="config">The configuration to initialise with.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddBlobStorageSingleton(this IServiceCollection services, ConnectionConfig config)
        {
            services.AddSingleton<IBlobStorage>(new BlobStorage(config));
            services.AddFactoryIfNotAdded<IBlobStorage>();
            return services;
        }

    }
}
