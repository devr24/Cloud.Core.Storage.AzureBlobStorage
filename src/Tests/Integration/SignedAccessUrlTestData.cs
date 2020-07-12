namespace Cloud.Core.Storage.AzureBlobStorage.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using Xunit;

    public class SignedAccessUrlTestData : TheoryData<Dictionary<string, string>, ISignedAccessConfig, string>
    {
        public SignedAccessUrlTestData()
        {
            var oneDayExpiry = DateTime.UtcNow.AddDays(1);
            var threeDaysExpiry = DateTime.UtcNow.AddDays(3);
            var oneWeekExpiry = DateTime.UtcNow.AddDays(7);
            var testFolderName = "testing/test-folder-name";
            var testSubFolderName = "testing/test-folder-name/test-sub-folder";
            var testFolderPathContainerOnly = "testing";

            Add(new Dictionary<string, string>
            {
                {"ExpiryDate", oneDayExpiry.Date.ToString("yyyy-MM-dd")},
                {"Permission", "sp=a" },
                {"FolderPath", testFolderName }
            },
            new SignedAccessConfig(new List<AccessPermission> { AccessPermission.Add }, oneDayExpiry),
            testFolderName);

            Add(new Dictionary<string, string>
                {
                    {"ExpiryDate", threeDaysExpiry.Date.ToString("yyyy-MM-dd")},
                    {"Permission", "sp=c" },
                    {"FolderPath", testFolderPathContainerOnly }
                },
                new SignedAccessConfig(new List<AccessPermission> { AccessPermission.Create }, threeDaysExpiry),
                testFolderPathContainerOnly);

            Add(new Dictionary<string, string>
                {
                    {"ExpiryDate", threeDaysExpiry.Date.ToString("yyyy-MM-dd")},
                    {"Permission", "sp=c" },
                    {"FolderPath", testSubFolderName }
                },
            new SignedAccessConfig(new List<AccessPermission> { AccessPermission.Create }, threeDaysExpiry),
            testSubFolderName);

            Add(new Dictionary<string, string>
                {
                    {"ExpiryDate", oneWeekExpiry.Date.ToString("yyyy-MM-dd")},
                    {"Permission", "sp=d" },
                    {"FolderPath", testFolderName }
                },
            new SignedAccessConfig(new List<AccessPermission> { AccessPermission.Delete }, oneWeekExpiry),
            testFolderName);

            Add(new Dictionary<string, string>
                {
                    {"ExpiryDate", oneDayExpiry.Date.ToString("yyyy-MM-dd")},
                    {"Permission", "sp=l" },
                    {"FolderPath", testSubFolderName }
                },
            new SignedAccessConfig(new List<AccessPermission> { AccessPermission.List }, oneDayExpiry),
            testSubFolderName);

            Add(new Dictionary<string, string>
                {
                    {"ExpiryDate", oneWeekExpiry.Date.ToString("yyyy-MM-dd")},
                    {"Permission", "sp=l" },
                    {"FolderPath", testSubFolderName }
                },
            new SignedAccessConfig(new List<AccessPermission> { AccessPermission.List, AccessPermission.List }, oneWeekExpiry),
            testSubFolderName);

            Add(new Dictionary<string, string>
                {
                    {"ExpiryDate", threeDaysExpiry.Date.ToString("yyyy-MM-dd")},
                    {"Permission", "sp=rw" },
                    {"FolderPath", testSubFolderName }
                },
            new SignedAccessConfig(new List<AccessPermission> { AccessPermission.Read, AccessPermission.Write }, threeDaysExpiry),
            testSubFolderName);

            Add(new Dictionary<string, string>
                {
                    {"ExpiryDate", oneWeekExpiry.Date.ToString("yyyy-MM-dd")},
                    {"Permission", "sp=raw" },
                    {"FolderPath", testSubFolderName }
                },
            new SignedAccessConfig(new List<AccessPermission> { AccessPermission.Add, AccessPermission.Read, AccessPermission.Write }, oneWeekExpiry),
            testSubFolderName);
        }
    }
}
