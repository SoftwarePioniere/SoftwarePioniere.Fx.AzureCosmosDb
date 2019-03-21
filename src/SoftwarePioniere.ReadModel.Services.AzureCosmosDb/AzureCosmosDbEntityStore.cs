using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace SoftwarePioniere.ReadModel.Services.AzureCosmosDb
{
    public class AzureCosmosDbEntityStore : EntityStoreBase<AzureCosmosDbOptions>
    {
        private readonly AzureCosmosDbConnectionProvider _provider;

        public AzureCosmosDbEntityStore(AzureCosmosDbOptions options,
            AzureCosmosDbConnectionProvider provider) : base(options)
        {
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        public override async Task<T[]> LoadItemsAsync<T>(CancellationToken token = default(CancellationToken))
        {

            Logger.LogTrace("LoadItemsAsync: {EntityType}", typeof(T));

            token.ThrowIfCancellationRequested();

            var feedOptions = new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = false };

            //type filtern wegen partition key verletzungen
            var source = _provider.CreateQuery<T>(feedOptions).Where(x => x.EntityType == TypeKeyCache.GetEntityTypeKey<T>());
            var xx = await source.ToArrayAsync(token, Logger).ConfigureAwait(false);
            return xx;
        }

        public override async Task<T[]> LoadItemsAsync<T>(Expression<Func<T, bool>> where, CancellationToken token = default(CancellationToken))
        {
            Logger.LogTrace("LoadItemsAsync: {EntityType} {Expression}", typeof(T), where);

            token.ThrowIfCancellationRequested();


            var feedOptions = new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = false };

            //type filtern wegen partition key verletzungen
            var source = _provider.CreateQuery<T>(feedOptions).Where(x => x.EntityType == TypeKeyCache.GetEntityTypeKey<T>()).Where(where);
            var xx = await source.ToArrayAsync(token, Logger).ConfigureAwait(false);
            return xx;

        }

        public override Task<PagedResults<T>> LoadPagedResultAsync<T>(PagedLoadingParameters<T> parms, CancellationToken token = default(CancellationToken))
        {
            //TODO: FIX
            throw new NotImplementedException();

            //if (parms == null)
            //{
            //    throw new ArgumentNullException(nameof(parms));
            //}

            //if (Logger.IsEnabled(LogLevel.Debug))
            //{
            //    Logger.LogDebug("LoadPagedResultAsync: {EntityType} {Paramter}", typeof(T), parms);
            //}
            //token.ThrowIfCancellationRequested();


            //var feedOptions = new FeedOptions() { MaxItemCount = parms.PageSize, EnableCrossPartitionQuery = false };
            //if (!string.IsNullOrEmpty(parms.ContinuationToken))
            //{
            //    feedOptions.RequestContinuation = parms.ContinuationToken;
            //}

            ////type filtern wegen partition key verletzungen
            //var query = _provider.CreateQuery<T>(feedOptions).Where(x => x.EntityType == TypeKeyCache.GetEntityTypeKey<T>());
            //if (parms.Where != null)
            //    query = query.Where(parms.Where);

            //if (parms.OrderByDescending != null)
            //    query = query.OrderByDescending(parms.OrderByDescending);

            //if (parms.OrderBy != null)
            //    query = query.OrderBy(parms.OrderBy);

            //var jo = JObject.Parse(query.ToString());
            //var countQuerySql = jo["query"].Value<string>();
            //countQuerySql = countQuerySql.Replace("*", "VALUE COUNT(1)");

            //var countQuery = _provider.Client.Value.CreateDocumentQuery<int>(_provider.CollectionUri, new SqlQuerySpec(countQuerySql));
            //token.ThrowIfCancellationRequested();

            //var totalCount = await countQuery.TakeOneAsync(token).ConfigureAwait(false);

            //var res = await query.ToPagedResultsAsync(token).ConfigureAwait(false);

            //return new PagedResults<T>
            //{
            //    ContinuationToken = res.ContinuationToken,
            //    Page = parms.Page,
            //    PageSize = parms.PageSize,
            //    ResultCount = res.Results.Count,
            //    Results = res.Results,
            //    TotalCount = totalCount
            //};

        }

        protected override async Task InternalDeleteItemAsync<T>(string entityId, CancellationToken token = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(entityId))
            {
                throw new ArgumentNullException(nameof(entityId));
            }

            Logger.LogTrace("InternalDeleteItemAsync: {EntityType} {EntityId}", typeof(T), entityId);

            token.ThrowIfCancellationRequested();

            await _provider.DeleteItemAsync(entityId, TypeKeyCache.GetEntityTypeKey<T>());
        }

        protected override async Task InternalInsertItemAsync<T>(T item, CancellationToken token = default(CancellationToken))
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }


            Logger.LogTrace("InternalInsertItemAsync: {EntityType} {EntityId}", typeof(T), item.EntityId);

            token.ThrowIfCancellationRequested();

            await _provider.AddItemAsync(item).ConfigureAwait(false);
        }

        protected override async Task InternalBulkInsertItemsAsync<T>(T[] items, CancellationToken token = new CancellationToken())
        {
            if (items == null)
            {
                throw new ArgumentNullException(nameof(items));
            }

            Logger.LogTrace("BulkInsertItemsAsync: {EntityType} {EntityCount}", typeof(T), items.Length);

            var client = _provider.Client.Value;
            try
            {
                // Set retries to 0 to pass complete control to bulk executor.
                client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
                client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;

                IBulkExecutor bulkExecutor = new BulkExecutor(client, _provider.GetCollection());
                await bulkExecutor.InitializeAsync();

                var bulkImportResponse = await bulkExecutor.BulkImportAsync(
                    documents: items,
                    enableUpsert: true,
                    disableAutomaticIdGeneration: true,
                    maxConcurrencyPerPartitionKeyRange: null,
                    maxInMemorySortingBatchSize: null,
                    cancellationToken: token);

                Logger.LogTrace(
                    "BulkImportAsync: Imported: {NumberOfDocumentsImported} / RequestUnits: {RequestCharge} / TimeTaken {TotalTimeTaken}",
                    bulkImportResponse.NumberOfDocumentsImported,
                    bulkImportResponse.TotalRequestUnitsConsumed,
                    bulkImportResponse.TotalTimeTaken);

                if (bulkImportResponse.BadInputDocuments != null && bulkImportResponse.BadInputDocuments.Any())
                {
                    Logger.LogWarning("BulkImport Bad Documents");
                    foreach (var o in bulkImportResponse.BadInputDocuments)
                    {
                        Logger.LogWarning("BulkImport Bad Doc {@doc}", o);
                    }

                    throw new InvalidOperationException("Bulk Import Bad Documents");

                }
            }
            finally
            {

                client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
                client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;
            }

        }

        protected override async Task InternalInsertOrUpdateItemAsync<T>(T item, CancellationToken token = default(CancellationToken))
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            Logger.LogTrace("InternalInsertOrUpdateItemAsync: {EntityType} {EntityId}", typeof(T), item.EntityId);

            token.ThrowIfCancellationRequested();


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

            Logger.LogTrace("InternalLoadItemAsync: {EntityType} {EntityId}", typeof(T), entityId);

            token.ThrowIfCancellationRequested();

            try
            {

                //var feedOptions = new FeedOptions() { EnableCrossPartitionQuery = false };
                ////type filtern wegen partition key verletzungen
                //var query = _provider.CreateQuery<T>(feedOptions).Where(x => x.EntityType == TypeKeyCache.GetEntityTypeKey<T>());
                //query = query.Where(x => x.EntityId == entityId);

                //var jo = JObject.Parse(query.ToString());
                //var countQuerySql = jo["query"].Value<string>();
                //countQuerySql = countQuerySql.Replace("*", "VALUE COUNT(1)");

                //var countQuerySql = $"SELECT VALUE COUNT(1) FROM root WHERE ((root[\"entity_type\"] = \"{TypeKeyCache.GetEntityTypeKey<T>()}\") AND (root[\"id\"] = \"{entityId}\"))";
                //var countQuery = _provider.Client.Value.CreateDocumentQuery<int>(_provider.CollectionUri, new SqlQuerySpec(countQuerySql));
                //token.ThrowIfCancellationRequested();

                //var totalCount = await countQuery.TakeOneAsync(token).ConfigureAwait(false);

                //if (totalCount == 0)
                //{
                //    Logger.LogDebug("No Value count");
                //    return null;
                //}

                var response = await _provider.Client.Value.ReadDocumentAsync(
                    _provider.GetDocumentLink(entityId),
                    new RequestOptions { PartitionKey = new PartitionKey(TypeKeyCache.GetEntityTypeKey<T>()) },
                    token);

                Logger.LogTrace("ReadDocumentAsync: EntityId: {EntityId} / StatusCode: {StatusCode} / RequestUnits: {RequestCharge} ", entityId,
                    response.StatusCode, response.RequestCharge);

                var result = response.Resource.ToString();
                var item = JsonConvert.DeserializeObject<T>(result);
                return item;
            }
            catch (DocumentClientException exnf) when (exnf.Error.Code == "NotFound")
            {
                Logger.LogWarning("Entity with Id not found {EntityId}", entityId);
                return null;
            }
            catch (DocumentClientException e)
            {
                Logger.LogError(e, "Error in CosmosDb Load {ErrorCode} {Message}", e.Error.Code, e.Message);
                throw;
            }

        }

        protected override async Task InternalUpdateItemAsync<T>(T item, CancellationToken token = default(CancellationToken))
        {

            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            Logger.LogTrace("InternalUpdateItemAsync: {EntityType} {EntityId}", typeof(T), item.EntityId);

            token.ThrowIfCancellationRequested();


            await _provider.UpdateItemAsync(item, item.EntityId);
        }
    }
}