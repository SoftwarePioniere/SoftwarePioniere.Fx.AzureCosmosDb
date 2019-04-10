using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SoftwarePioniere.ReadModel.Services.AzureCosmosDb.Tests
{
    // ReSharper disable once ClassNeverInstantiated.Global
    public class TestCollectionFixture : IDisposable
    {
        public void Dispose()
        {
            var services = new ServiceCollection();
            services
                .AddOptions()
                .AddSingleton<ILoggerFactory>(new NullLoggerFactory())
                .AddAzureCosmosDbEntityStore(options => new TestConfiguration().ConfigurationRoot.Bind("AzureCosmosDb", options));

            var provider = services.BuildServiceProvider().GetService<AzureCosmosDbConnectionProvider>();
            provider.DeleteDocumentCollectionAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }
    }
}