# **Cloud.Core.Storage.AzureBlobStorage**

<div id="description">

An Azure specific implementation of blob storage and blob storage item.

</div>

## Design

One of the patterns used within this package (specifically when listing blobs) is the observable pattern.  This is possible because results (calls to the ListBlobs API) are yielded
as an observable.  You can read more on the observable pattern here: https://docs.microsoft.com/en-us/dotnet/standard/events/observer-design-pattern


## Usage

There are three ways you can instantiate the Blob Storage Client.  Each way dictates the security mechanism the client uses to connect.  The three mechanisms are:

1. Connection String
2. Service Principle
3. Managed Service Identity

Below are examples of instantiating each type.

### 1. Connection String
Create an instance of the Blob Storage client with ConnectionConfig for connection string as follows:

```csharp
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

```csharp
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

```csharp
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

**Note** - when requesting blobs you can specify an exact path in the following format:  `rootContainerName/somefolder/anotherfolder`

## Test Coverage
A threshold will be added to this package to ensure the test coverage is above 80% for branches, functions and lines.  If it's not above the required threshold 
(threshold that will be implemented on ALL of the new core repositories going forward), then the build will fail.

## Compatibility
This package has has been written in .net Standard and can be therefore be referenced from a .net Core or .net Framework application. The advantage of utilising from a .net Core application, 
is that it can be deployed and run on a number of host operating systems, such as Windows, Linux or OSX.  Unlike referencing from the a .net Framework application, which can only run on 
Windows (or Linux using Mono).
 
## Setup
This package requires the .net Core 2.1 SDK, it can be downloaded here: 
https://www.microsoft.com/net/download/dotnet-core/2.1

IDE of Visual Studio or Visual Studio Code, can be downloaded here:
https://visualstudio.microsoft.com/downloads/

## How to access this package
All of the Cloud.Core.* packages are published to our internal NuGet feed.  To consume this on your local development machine, please add the following feed to your feed sources in Visual Studio:
TBC

For help setting up, follow this article: https://docs.microsoft.com/en-us/vsts/package/nuget/consume?view=vsts
