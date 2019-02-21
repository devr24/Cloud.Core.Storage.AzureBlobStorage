namespace Cloud.Core.Storage.AzureBlobStorage.Config
{
    using System;

    /// <summary>
    /// Msi Configuration for Azure KeyVault.
    /// </summary>
    public class MsiConfig
    {

        /// <summary>
        /// Gets or sets the name of the key vault instance.
        /// </summary>
        /// <value>
        /// The name of the key vault instance.
        /// </value>
        public string StorageInstanceName { get; set; }
        
        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        /// <value>
        /// The tenant identifier.
        /// </value>
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the subscription identifier.
        /// </summary>
        /// <value>
        /// The subscription identifier.
        /// </value>
        public string SubscriptionId { get; set; }

        /// <summary>Ensure mandatory properties are set.</summary>
        public void Validate()
        {
            if (StorageInstanceName.IsNullOrEmpty())
                throw new ArgumentException("StorageInstanceName must be set");

            if (TenantId.IsNullOrEmpty())
                throw new ArgumentException("TenantId must be set");

            if (SubscriptionId.IsNullOrEmpty())
                throw new ArgumentException("SubscriptionId must be set");
        }
    }

    /// <summary>Connection string config.</summary>
    public class ConnectionConfig
    {
        /// <summary>
        /// Gets or sets the connection string for connecting to storage.
        /// </summary>
        /// <value>
        /// Storage connection string.
        /// </value>
        public string ConnectionString { get; set; }

        /// <summary>Ensure mandatory properties are set.</summary>
        public void Validate()
        {
            if (ConnectionString.IsNullOrEmpty())
                throw new ArgumentException("ConnectionString must be set");
        }
    }

    /// <summary>
    /// Service Principle Configuration for Azure KeyVault.
    /// </summary>
    public class ServicePrincipleConfig
    {
        /// <summary>
        /// Gets or sets the application identifier.
        /// </summary>
        /// <value>
        /// The application identifier.
        /// </value>
        public string AppId { get; set; }

        /// <summary>
        /// Gets or sets the application secret.
        /// </summary>
        /// <value>
        /// The application secret string.
        /// </value>
        public string AppSecret { get; set; }

        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        /// <value>
        /// The tenant identifier.
        /// </value>
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the subscription identifier.
        /// </summary>
        /// <value>
        /// The subscription identifier.
        /// </value>
        public string SubscriptionId { get; set; }

        /// <summary>
        /// Gets or sets the name of the storage instance.
        /// </summary>
        /// <value>
        /// The name of the storage instance.
        /// </value>
        public string StorageInstanceName { get; set; } 

        /// <summary>
        /// Gets the length of time (in seconds) a lock should be applied by default.
        /// </summary>
        /// <value>
        /// The lock time in seconds.
        /// If not set, defaults to 60 seconds.
        /// </value>
        public int LockInSeconds { get; set; } = 60;
        
        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return $"AppId: {AppId}, TenantId: {TenantId}, StorageInstanceName: {StorageInstanceName}, LockInSeconds: {LockInSeconds}";
        }

        /// <summary>Ensure mandatory properties are set.</summary>
        public void Validate()
        {
            if (StorageInstanceName.IsNullOrEmpty())
                throw new ArgumentException("StorageInstanceName must be set");

            if (AppId.IsNullOrEmpty())
                throw new ArgumentException("AppId must be set");

            if (AppSecret.IsNullOrEmpty())
                throw new ArgumentException("AppSecret must be set");

            if (TenantId.IsNullOrEmpty())
                throw new ArgumentException("TenantId must be set");

            if (SubscriptionId.IsNullOrEmpty())
                throw new ArgumentException("SubscriptionId must be set");
        }
    }
}
