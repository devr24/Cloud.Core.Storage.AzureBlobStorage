namespace Cloud.Core.Storage.AzureBlobStorage.Config
{
    using System;
    using System.Linq;
    using System.ComponentModel.DataAnnotations;
    using Validation;

    /// <summary>
    /// Msi Configuration for Azure Blob Storage.
    /// </summary>
    public class MsiConfig : ConfigBase
    {
        /// <summary>
        /// Gets or sets the name of the blob storage instance.
        /// </summary>
        /// <value>
        /// The name of the blob storage instance.
        /// </value>
        [Required]
        public string InstanceName { get; set; }

        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        /// <value>
        /// The tenant identifier.
        /// </value>
        [Required]
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the subscription identifier.
        /// </summary>
        /// <value>
        /// The subscription identifier.
        /// </value>
        [Required]
        public string SubscriptionId { get; set; }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return $"TenantId: {TenantId}, SubscriptionId:{SubscriptionId} Blob storage InstanceName: {InstanceName}, LockInSeconds: {LockInSeconds}, CreateIfNotExists: {CreateFolderIfNotExists}";
        }
    }

    /// <summary>Connection string config.</summary>
    public class ConnectionConfig : ConfigBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionConfig"/> class using default constructor.
        /// </summary>
        public ConnectionConfig()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionConfig"/> class with a connection string.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public ConnectionConfig(string connectionString)
        {
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Table storage instance name taken from the connection string.
        /// </summary>
        public string InstanceName
        {
            get
            {
                if (ConnectionString.IsNullOrEmpty())
                    return null;

                const string replaceStr = "AccountName=";

                var parts = ConnectionString.Split(';');

                if (parts.Length <= 1) {
                    return null;
                }

                // Account name is used as the identifier.
                return parts
                    .FirstOrDefault(p => p.StartsWith(replaceStr))?.Replace(replaceStr, string.Empty);
            }
        }

        /// <summary>
        /// Gets or sets the connection string for connecting to storage.
        /// </summary>
        /// <value>
        /// Storage connection string.
        /// </value>
        [Required]
        public string ConnectionString { get; set; }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return $"ConnectionString: {(ConnectionString.IsNullOrEmpty() ? "Not Set" : "Set")}, Blob storage InstanceName: {InstanceName}, LockInSeconds: {LockInSeconds}, CreateIfNotExists: {CreateFolderIfNotExists}";
        }
    }

    /// <summary>
    /// Service Principle Configuration for Azure blob storage.
    /// </summary>
    public class ServicePrincipleConfig : ConfigBase
    {
        /// <summary>
        /// Gets or sets the application identifier.
        /// </summary>
        /// <value>
        /// The application identifier.
        /// </value>
        [Required]
        public string AppId { get; set; }

        /// <summary>
        /// Gets or sets the application secret.
        /// </summary>
        /// <value>
        /// The application secret string.
        /// </value>
        [Required]
        public string AppSecret { get; set; }

        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        /// <value>
        /// The tenant identifier.
        /// </value>
        [Required]
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the subscription identifier.
        /// </summary>
        /// <value>
        /// The subscription identifier.
        /// </value>
        [Required]
        public string SubscriptionId { get; set; }

        /// <summary>
        /// Gets or sets the name of the blob storage instance.
        /// </summary>
        /// <value>
        /// The name of the blob storage instance.
        /// </value>
        [Required]
        public string InstanceName { get; set; } 

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return $"AppId: {AppId}, AppSecret: {(AppSecret.IsNullOrEmpty() ? "Not Set" : "Set")}, TenantId: {TenantId}, SubscriptionId: {SubscriptionId}, Blob storage InstanceName: {InstanceName}, LockInSeconds: {LockInSeconds}, CreateIfNotExists: {CreateFolderIfNotExists}";
        }
    }

    /// <summary>
    /// Common config application to all authentication types.
    /// </summary>
    public abstract class ConfigBase: AttributeValidator
    {
        /// <summary>
        /// Gets the length of time (in seconds) a lock should be applied by default.
        /// </summary>
        /// <value>
        /// The lock time in seconds.
        /// If not set, defaults to 60 seconds.
        /// </value>
        public int LockInSeconds { get; set; } = 60;

        /// <summary>
        /// Default behaviour - create a folder if it does not exist during upload.
        /// </summary>
        public bool CreateFolderIfNotExists { get; set; }
    }
}
