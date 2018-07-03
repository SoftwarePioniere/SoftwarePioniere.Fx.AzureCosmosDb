using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Caching;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace SoftwarePioniere.ReadModel.Services.AzureCosmosDb
{
    public class AzureCosmosDbEntityStore : EntityStoreBase
    {
        private readonly AzureCosmosDbConnectionProvider _provider;

        public AzureCosmosDbEntityStore(ILoggerFactory loggerFactory, ICacheClient cacheClient,
            AzureCosmosDbConnectionProvider provider) : base(loggerFactory, cacheClient)
        {
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        public override async Task<T[]> LoadItemsAsync<T>(CancellationToken token = default(CancellationToken))
        {
            if (Logger.IsEnabled(LogLevel.Debug))
            {
                Logger.LogDebug("LoadItemsAsync: {EntityType}", typeof(T));
            }

            var feedOptions = new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = false };

            //type filtern wegen partition key verletzungen
            var source = _provider.CreateQuery<T>(feedOptions).Where(x => x.EntityType == TypeKeyCache.GetEntityTypeKey<T>());
            var xx = await source.ToArrayAsync(token).ConfigureAwait(false);
            return xx;
        }

        public override async Task<T[]> LoadItemsAsync<T>(Expression<Func<T, bool>> where, CancellationToken token = default(CancellationToken))
        {
            if (Logger.IsEnabled(LogLevel.Debug))
            {
                Logger.LogDebug("LoadItemsAsync: {EntityType} {Expression}", typeof(T), where);
            }


            var feedOptions = new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = false };

            //type filtern wegen partition key verletzungen
            var source = _provider.CreateQuery<T>(feedOptions).Where(x => x.EntityType == TypeKeyCache.GetEntityTypeKey<T>()).Where(where);
            var xx = await source.ToArrayAsync(token).ConfigureAwait(false);
            return xx;

        }

        public override async Task<PagedResults<T>> LoadPagedResultAsync<T>(PagedLoadingParameters<T> parms, CancellationToken token = default(CancellationToken))
        {
            if (parms == null)
            {
                throw new ArgumentNullException(nameof(parms));
            }

            if (Logger.IsEnabled(LogLevel.Debug))
            {
                Logger.LogDebug("LoadPagedResultAsync: {EntityType} {Paramter}", typeof(T), parms);
            }


            var feedOptions = new FeedOptions() { MaxItemCount = parms.PageSize, EnableCrossPartitionQuery = false };
            if (!string.IsNullOrEmpty(parms.ContinuationToken))
            {
                feedOptions.RequestContinuation = parms.ContinuationToken;
            }

            //type filtern wegen partition key verletzungen
            var query = _provider.CreateQuery<T>(feedOptions).Where(x => x.EntityType == TypeKeyCache.GetEntityTypeKey<T>());
            if (parms.Where != null)
                query = query.Where(parms.Where);

            if (parms.OrderByDescending != null)
                query = query.OrderByDescending(parms.OrderByDescending);

            if (parms.OrderBy != null)
                query = query.OrderBy(parms.OrderBy);

            var jo = JObject.Parse(query.ToString());
            var countQuerySql = jo["query"].Value<string>();
            countQuerySql = countQuerySql.Replace("*", "VALUE COUNT(1)");

            var countQuery = _provider.Client.Value.CreateDocumentQuery<int>(_provider.CollectionUri, new SqlQuerySpec(countQuerySql));

            var totalCount = await countQuery.TakeOneAsync(token).ConfigureAwait(false);

            var res = await query.ToPagedResultsAsync(token).ConfigureAwait(false);

            return new PagedResults<T>
            {
                ContinuationToken = res.ContinuationToken,
                Page = parms.Page,
                PageSize = parms.PageSize,
                ResultCount = res.Results.Count,
                Results = res.Results,
                TotalCount = totalCount
            };

        }

        protected override async Task InternalDeleteItemAsync<T>(string entityId, CancellationToken token = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(entityId))
            {
                throw new ArgumentNullException(nameof(entityId));
            }

            if (Logger.IsEnabled(LogLevel.Debug))
            {
                Logger.LogDebug("InternalDeleteItemAsync: {EntityType} {EntityId}", typeof(T), entityId);
            }

            await _provider.DeleteItemAsync(entityId, TypeKeyCache.GetEntityTypeKey<T>());
        }

        protected override async Task InternalInsertItemAsync<T>(T item, CancellationToken token = default(CancellationToken))
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            if (Logger.IsEnabled(LogLevel.Debug))
            {
                Logger.LogDebug("InternalInsertItemAsync: {EntityType} {EntityId}", typeof(T), item.EntityId);
            }

            await _provider.AddItemAsync(item).ConfigureAwait(false);
        }

        protected override async Task InternalInsertOrUpdateItemAsync<T>(T item, CancellationToken token = default(CancellationToken))
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            if (Logger.IsEnabled(LogLevel.Debug))
            {
                Logger.LogDebug("InternalInsertOrUpdateItemAsync: {EntityType} {EntityId}", typeof(T), item.EntityId);
            }

            var exi = await _provider.ExistsDocument<T>(item.EntityId);
            if (exi)
            {
                await UpdateItemAsync(item, token).ConfigureAwait(false);
            }
            else
            {
                await InsertItemAsync(item, token).ConfigureAwait(false);
            }
        }

        protected override async Task<T> InternalLoadItemAsync<T>(string entityId, CancellationToken token = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(entityId))
            {
                throw new ArgumentNullException(nameof(entityId));
            }

            if (Logger.IsEnabled(LogLevel.Debug))
            {
                Logger.LogDebug("InternalLoadItemAsync: {EntityType} {EntityId}", typeof(T), entityId);
            }

            try
            {
                var response = await _provider.Client.Value.ReadDocumentAsync(
                    _provider.GetDocumentLink(entityId),
                    new RequestOptions { PartitionKey = new PartitionKey(TypeKeyCache.GetEntityTypeKey<T>()) });

                var result = response.Resource.ToString();
                var item = JsonConvert.DeserializeObject<T>(result);
                return item;
            }
            catch (DocumentClientException e)
            {
                if (e.Error.Code == "NotFound")
                {
                    return null;
                }

                throw;
            }

        }

        protected override async Task InternalUpdateItemAsync<T>(T item, CancellationToken token = default(CancellationToken))
        {

            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            if (Logger.IsEnabled(LogLevel.Debug))
            {
                Logger.LogDebug("InternalUpdateItemAsync: {EntityType} {EntityId}", typeof(T), item.EntityId);
            }

            await _provider.UpdateItemAsync(item, item.EntityId);
        }
    }
}