# **Cloud.Core.Storage.AzureBlobStorage** 
[![Build status](https://dev.azure.com/cloudcoreproject/CloudCore/_apis/build/status/Cloud.Core%20Packages/Cloud.Core.Storage.AzureBlobStorage_Package)](https://dev.azure.com/cloudcoreproject/CloudCore/_build/latest?definitionId=15)
![Code Coverage](https://cloud1core.blob.core.windows.net/codecoveragebadges/Cloud.Core.Storage.AzureBlobStorage-LineCoverage.png) 
[![Cloud.Core.Storage.AzureBlobStorage package in Cloud.Core feed in Azure Artifacts](https://feeds.dev.azure.com/cloudcoreproject/dfc5e3d0-a562-46fe-8070-7901ac8e64a0/_apis/public/Packaging/Feeds/8949198b-5c74-42af-9d30-e8c462acada6/Packages/c5808ae6-e7d0-4aef-affb-601e0cbc86ad/Badge)](https://dev.azure.com/cloudcoreproject/CloudCore/_packaging?_a=package&feed=8949198b-5c74-42af-9d30-e8c462acada6&package=c5808ae6-e7d0-4aef-affb-601e0cbc86ad&preferRelease=true)



<div id="description">

An Azure specific implementation of blob storage and blob storage item.

</div>

## Design

One of the patterns used within this package (specifically when listing blobs) is the observable pattern.  This is possible because results (calls to the ListBlobs API) are yielded as an observable.  You can read more on the observable pattern here: https://docs.microsoft.com/en-us/dotnet/standard/events/observer-design-pattern


## Usage

There are three ways you can instantiate the Blob Storage Client.  Each way dictates the security mechanism the client uses to connect.  The three mechanisms are:

1. Connection String
2. Service Principle
3. Managed Service Identity

Below are examples of instantiating each type.

### 1. Connection String
Create an instance of the Blob Storage client with ConnectionConfig for connection string as follows:

```
var blobConfig = new ConnectionConfig
    {
        ConnectionString = "<connectionstring>"
    };

// Blob client.
var blobStorage = new BlobStorage(blobConfig);	
```
Note: Instance name not required to be specified anywhere in configuration here as it is taken from the connection string.

### 2. Service Principle
Create an instance of the Blob Storage client with BlobStorageConfig for Service Principle as follows:

```
var blobConfig = new ServicePrincipleConfig
    {
        AppId = "<appid>",
        AppSecret = "<appsecret>",
        TenantId = "<tenantid>",
        StorageInstanceName = "<storageinstancename>",
        SubscriptionId = subscriptionId
    };

// Blob client.
var blobStorage = new BlobStorage(blobConfig);	
```

Usually the AppId, AppSecret (both of which are setup when creating a new service principle within Azure) and TenantId are specified in 
Configuration (environment variables/AppSetting.json file/key value pair files [for Kubernetes secret store] or command line arguments).

SubscriptionId can be accessed through the secret store (this should not be stored in configuration).

### 3. Management Service Idenity (MSI)
Create an instance of the Blob Storage client with MSI authentication as follows:

```
var blobConfig = new MsiConfig
    {
        TenantId = "<tenantid>",
        StorageInstanceName = "<storageinstancename>",
        SubscriptionId = subscriptionId
    };

// Blob client.
var blobStorage = new BlobStorage(blobConfig);	
```

All that's required is the instance name to connect to.  Authentication runs under the context the application is running.


### Getting Blobs
The following shows how to executes methods on the storage client (client instantiated in examples above).  The code below enumerates all blobs in a given container:

```csharp
// Read list of blobs within a container (including sub-folders).
var blobs = await blobStorage.ListBlobs("containerName", true);

// Output each blob name.
foreach (var blobItem in blobs)
{
    Console.WriteLine("Blob item: " + blobItem.FileName);
}
```

> When requesting blobs you can specify an exact path in the following format:  `rootContainerName/somefolder/anotherfolder`

### Blob Properties
Blobs in Azure have a last modified date and any uploaded will have an additional metadata property "LastWriteTimeUTC". We will attempt to retreive the "LastWriteTimeUTC" first and if it is not there fall back to the LastModifiedDate of the blob. Incase this is not set either, the LastModified property on the blob will be instantiated to DateTime.MinValue.

### Check Blob exists

The following code can be used to check if a blob exists in storage:

```csharp
var exists = await _blobStorage.Exists("MyContainer/SampleText.txt");
```

### Download a Blob

The following code shows an example of downloading text:

```csharp
var sourcePath = "MyContainer/SampleText.txt";
var text = string.Empty;

// Download
using (var blobStream = await _blobStorage.DownloadBlob(sourcePath))
{
	if (blobStream.Length > 0)
	{
		text = blobStream.ConvertToString();
	}
}               
```

### Upload a Blob

The following code shows an example of uploading text:

```csharp
var targetPath = "MyContainer/SampleText.txt";
var sampleText = "Hello world";
var textStream = sampleText.ConvertToStream(Encoding.UTF8); // you can use your own stream here

// Upload
await _blobStorage.UploadBlob(targetPath, textStream);
```

**Note** - Do not update the Microsoft.IdentityModel.Clients.ActiveDirectory package.  It should be set to version 3.19.8.  This is the only package which overlaps between other Cloud.Core packages and must be kept inline (either update all or leave all as is currently).

## Test Coverage
A threshold will be added to this package to ensure the test coverage is above 80% for branches, functions and lines.  If it's not above the required threshold 
(threshold that will be implemented on ALL of the core repositories to gurantee a satisfactory level of testing), then the build will fail.

## Compatibility
This package has has been written in .net Standard and can be therefore be referenced from a .net Core or .net Framework application. The advantage of utilising from a .net Core application, 
is that it can be deployed and run on a number of host operating systems, such as Windows, Linux or OSX.  Unlike referencing from the a .net Framework application, which can only run on 
Windows (or Linux using Mono).
 
## Setup
This package is built using .net Standard 2.1 and requires the .net Core 3.1 SDK, it can be downloaded here: 
https://www.microsoft.com/net/download/dotnet-core/

IDE of Visual Studio or Visual Studio Code, can be downloaded here:
https://visualstudio.microsoft.com/downloads/

## How to access this package
All of the Cloud.Core.* packages are published to a internal NuGet feed.  To consume this on your local development machine, please add the following feed to your feed sources in Visual Studio:
https://pkgs.dev.azure.com/cloudcoreproject/CloudCore/_packaging/Cloud.Core/nuget/v3/index.json
 
For help setting up, follow this article: https://docs.microsoft.com/en-us/vsts/package/nuget/consume?view=vsts


<img src="https://cloud1core.blob.core.windows.net/icons/cloud_core_small.PNG" />
