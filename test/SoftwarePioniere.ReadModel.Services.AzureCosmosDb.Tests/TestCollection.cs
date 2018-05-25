using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace SoftwarePioniere.ReadModel.Services.AzureCosmosDb.Tests
{
    
    [CollectionDefinition("AzureCosmosDbCollection")]
    public class TestCollection : ICollectionFixture<TestCollectionFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }

}
