﻿using System;
using Foundatio.Caching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SoftwarePioniere.ReadModel;
using SoftwarePioniere.ReadModel.Services.AzureCosmosDb;

// ReSharper disable once CheckNamespace
namespace SoftwarePioniere.Extensions.DependencyInjection
{
    public static class DependencyInjectionExtensions
    {
        public static IServiceCollection AddAzureCosmosDbEntityStore(this IServiceCollection services) =>
            services.AddAzureCosmosDbEntityStore(_ => { });

        public static IServiceCollection AddAzureCosmosDbEntityStore(this IServiceCollection services, Action<AzureCosmosDbOptions> configureOptions)
        {

            services
                .AddOptions()
                .Configure(configureOptions);

            //var settings = services.BuildServiceProvider().GetService<IOptions<AzureCosmosDbOptions>>().Value;

            services
                .AddSingleton<AzureCosmosDbConnectionProvider>()
                .AddSingleton<IEntityStoreConnectionProvider>(provider => provider.GetRequiredService<AzureCosmosDbConnectionProvider>())
                .AddSingleton<IEntityStore>(provider =>
                {
                    var options = provider.GetRequiredService<IOptions<AzureCosmosDbOptions>>().Value;
                    options.CacheClient = provider.GetRequiredService<ICacheClient>();
                    options.LoggerFactory = provider.GetRequiredService<ILoggerFactory>();

                    return new AzureCosmosDbEntityStore(options, provider.GetRequiredService<AzureCosmosDbConnectionProvider>());
                });

            return services;
        }
    }
}
