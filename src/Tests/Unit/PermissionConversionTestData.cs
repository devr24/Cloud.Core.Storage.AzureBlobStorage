namespace Cloud.Core.Storage.AzureBlobStorage.Tests.Unit
{
    using System.Collections.Generic;
    using Microsoft.Azure.Storage.Blob;
    using Xunit;

    public class PermissionConversionTestData : TheoryData<SharedAccessBlobPermissions, List<AccessPermission>>
    {
        public PermissionConversionTestData()
        {
            Add(SharedAccessBlobPermissions.Add, new List<AccessPermission> { AccessPermission.Add });
            Add(SharedAccessBlobPermissions.Create, new List<AccessPermission> { AccessPermission.Create });
            Add(SharedAccessBlobPermissions.Delete, new List<AccessPermission> { AccessPermission.Delete });
            Add(SharedAccessBlobPermissions.List, new List<AccessPermission> { AccessPermission.List });
            Add(SharedAccessBlobPermissions.Read, new List<AccessPermission> { AccessPermission.Read });
            Add(SharedAccessBlobPermissions.Write, new List<AccessPermission> { AccessPermission.Write });
            Add(SharedAccessBlobPermissions.Delete, new List<AccessPermission> { AccessPermission.Delete, AccessPermission.Delete });
            Add(SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write, new List<AccessPermission> { AccessPermission.Read, AccessPermission.Write });
            Add(SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Read, new List<AccessPermission> { AccessPermission.List, AccessPermission.Read });
            Add(SharedAccessBlobPermissions.Add | SharedAccessBlobPermissions.Create, new List<AccessPermission> { AccessPermission.Add, AccessPermission.Create });
            Add(SharedAccessBlobPermissions.Add | SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write, new List<AccessPermission> { AccessPermission.Add, AccessPermission.Read, AccessPermission.Write });
        }
    }
}
