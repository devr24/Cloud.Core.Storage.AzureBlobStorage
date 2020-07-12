using System;
using System.Collections.Generic;

namespace Cloud.Core.Storage.AzureBlobStorage.Tests.Integration
{
    internal class SignedAccessConfig : ISignedAccessConfig
    {
        public SignedAccessConfig(List<AccessPermission> access, DateTime expiry)
        {
            AccessPermissions = access;
            AccessExpiry = expiry;
        }

        public List<AccessPermission> AccessPermissions { get; set; }

        public DateTimeOffset? AccessExpiry { get; set; }
    }
}
