namespace Microsoft.Extensions.DependencyInjection
{
    using System;
    using System.Linq;
    using Cloud.Core;
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
                SubscriptionId = subscriptionId
            });

            if (!key.IsNullOrEmpty())
                instance.Name = key;

            services.AddSingleton<IBlobStorage>(instance);
            AddFactoryIfNotAdded(services);
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure Blob storage as a singleton, using managed user config to setup.  Requires the instance 
        /// name, TenantId and SubscriptionId to be supplied.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="instanceName">Name of the blob storage instance to connect to.</param>
        /// <param name="tenantId">Tenant Id the instance lives in.</param>
        /// <param name="subscriptionId">Subscription Id for the tenant.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddBlobStorageSingleton(this IServiceCollection services, string instanceName, string tenantId, string subscriptionId)
        {
            services.AddBlobStorageSingleton(new MsiConfig {
                InstanceName = instanceName, TenantId = tenantId, SubscriptionId = subscriptionId
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
            AddFactoryIfNotAdded(services);
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
            AddFactoryIfNotAdded(services);
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
            AddFactoryIfNotAdded(services);
            return services;
        }

        /// <summary>
        /// Add the generic service factory from Cloud.Core for the IBlobStorage type.  This allows multiple named instances of the same instance.
        /// </summary>
        /// <param name="services">Service collection to extend.</param>
        private static void AddFactoryIfNotAdded(IServiceCollection services)
        {
            if (!services.Any(x => x.ServiceType == typeof(NamedInstanceFactory<IBlobStorage>)))
            {
                // Service Factory doesn't exist, so we add it to ensure it's always available.
                services.AddSingleton<NamedInstanceFactory<IBlobStorage>>();
            }
        }
    }
}
