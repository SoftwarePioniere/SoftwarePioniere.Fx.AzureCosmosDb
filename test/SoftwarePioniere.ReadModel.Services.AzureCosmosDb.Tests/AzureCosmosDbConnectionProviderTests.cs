using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

namespace SoftwarePioniere.ReadModel.Services.AzureCosmosDb.Tests
{
    [Collection("AzureCosmosDbCollection")]
    public class AzureCosmosDbConnectionProviderTests : TestBase
    {
        public AzureCosmosDbConnectionProviderTests(ITestOutputHelper output) : base(output)
        {
            ServiceCollection
                .AddOptions()
                .AddAzureCosmosDbEntityStore(options => new TestConfiguration().ConfigurationRoot.Bind("AzureCosmosDb", options));
        }

        private AzureCosmosDbConnectionProvider CreateProvider()
        {
            return GetService<AzureCosmosDbConnectionProvider>();
        }

        [Fact]
        public async Task CanClearDatabase()
        {
            var provider = CreateProvider();

            await provider.Client.Value.OpenAsync();
            provider.CheckDatabaseExists().Should().BeTrue();
            provider.CheckCollectionExists().Should().BeTrue();

            await provider.ClearDatabaseAsync();
            provider.CheckDatabaseExists().Should().BeTrue();
            provider.CheckCollectionExists().Should().BeFalse();

            await provider.Client.Value.OpenAsync();
            provider.CheckDatabaseExists().Should().BeTrue();
            provider.CheckCollectionExists().Should().BeTrue();
        }


        [Fact]
        public async Task CanConnectToClient()
        {
            var provider = CreateProvider();
            await provider.Client.Value.OpenAsync();
        }
    }
}