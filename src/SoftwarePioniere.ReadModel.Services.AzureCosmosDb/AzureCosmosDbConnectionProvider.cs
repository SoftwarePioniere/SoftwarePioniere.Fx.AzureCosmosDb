using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SoftwarePioniere.ReadModel.Services.AzureCosmosDb
{
    public class AzureCosmosDbConnectionProvider : IEntityStoreConnectionProvider
    {

        private readonly ILogger _logger;
        public TypeKeyCache KeyCache { get; }

        public AzureCosmosDbConnectionProvider(ILoggerFactory loggerFactory,
            IOptions<AzureCosmosDbOptions> options)
        {
            if (loggerFactory == null) throw new ArgumentNullException(nameof(loggerFactory));
            _logger = loggerFactory.CreateLogger(GetType());

            Options = options.Value;
            _logger.LogInformation("AzureCosmosDb Connection {Connection}", options);
            KeyCache = new TypeKeyCache();
            CollectionUri = GetCollectionLink();
            InitClient();
        }
        public Uri CollectionUri { get; private set; }

        /// <summary>
        ///     Policy Config Änderung
        /// </summary>
        // ReSharper disable once UnusedAutoPropertyAccessor.Local
        private static Action<ConnectionPolicy> PolicyConfig { get; set; }

        private AzureCosmosDbOptions Options { get; }

        public async Task ClearDatabaseAsync()
        {
            _logger.LogInformation("Clear Database");
            await DeleteDocumentCollectionAsync();
            _logger.LogInformation("Reinit Client");
            InitClient();
        }


        public bool CheckCollectionExists()
        {
            using (var client = CreateClient())
            {
                var collection = client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(Options.DatabaseId))
                    .Where(c => c.Id == Options.CollectionId)
                    .ToArray()
                    .SingleOrDefault();

                if (collection != null)
                {
                    _logger.LogTrace("Collection {0} in Database {1} found", Options.CollectionId,
                        Options.DatabaseId);

                    return true;
                }
                _logger.LogTrace("Collection {0} in Database {1} not found", Options.CollectionId,
                    Options.DatabaseId);
                return false;
            }
        }


        public bool CheckDatabaseExists()
        {
            using (var client = CreateClient())
            {
                client.OpenAsync().ConfigureAwait(false).GetAwaiter().GetResult();

                var database = client.CreateDatabaseQuery().Where(c => c.Id == Options.DatabaseId).ToArray()
                    .FirstOrDefault();


                if (database != null)
                {
                    _logger.LogTrace("Database {0} found", Options.DatabaseId);
                    return true;
                }

                _logger.LogTrace("Database {0} not found", Options.DatabaseId);
                return false;
            }
        }

        private DocumentClient CreateClient()
        {
            var policy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Gateway,
                ConnectionProtocol = Protocol.Https,
                MaxConnectionLimit = 100
            };

            var endpoint = new Uri(Options.EndpointUrl);
            var client = new DocumentClient(endpoint, Options.AuthKey, policy);

            if (PolicyConfig != null)
            {
                _logger.LogTrace("Invoking PolicyConfig");
                PolicyConfig(policy);
            }

            return client;
        }

        private async Task CreateCollectionAsync()
        {
            using (var client = CreateClient())
            {
                //if (_settings.CreateObjectsIfNotExists)
                //{
                var collectionDefinition = new DocumentCollection { Id = Options.CollectionId };
                collectionDefinition.PartitionKey.Paths.Add("/entity_type");

                await client.CreateDocumentCollectionAsync(UriFactory.CreateDatabaseUri(Options.DatabaseId),
                    collectionDefinition).ConfigureAwait(false);
                _logger.LogInformation("Collection {0} in Database {1} created", Options.CollectionId,
                    Options.DatabaseId);

                //}
                //else
                //{
                //    logger.LogError("Collection {0} in Database {1} not found. Please create first",
                //        _settings.CollectionId,
                //        _settings.DatabaseId);
                //    throw new InvalidOperationException(
                //        $"Collection {_settings.CollectionId} in Database  {_settings.DatabaseId} not found. Please create first");
                //}
            }
        }

        private async Task CreateDatabaseAsync()
        {
            using (var client = CreateClient())
            {
                //if (settings.CreateObjectsIfNotExists)
                //{
                await client.CreateDatabaseAsync(new Database { Id = Options.DatabaseId }).ConfigureAwait(false);
                _logger.LogInformation("Database {0} created", Options.DatabaseId);

                //}
                //else
                //{
                //    logger.LogError("Database {0} not found. Please create first", _settings.DatabaseId);
                //    throw new InvalidOperationException($"Database {_settings.DatabaseId} not found. Please create first");
                //}
            }
        }


        public async Task DeleteDocumentCollectionAsync()
        {
            _logger.LogInformation("DeleteDocumentCollectionAsync");
            await Client.Value.DeleteDocumentCollectionAsync(CollectionUri).ConfigureAwait(false);
        }

        /// <summary>
        ///     Execute the function with retries on throttle
        /// </summary>
        /// <typeparam name="TV"></typeparam>
        /// <param name="function"></param>
        /// <returns></returns>
        private async Task<TV> ExecuteWithRetries<TV>(Func<Task<TV>> function)
        {
            //TODO: Implement with Polly
            //Policy                
            //    .Handle<InvalidOperationException>()


            while (true)
            {
                TimeSpan sleepTime;
                try
                {
                    return await function();
                }
                catch (DocumentClientException de)
                {
                    _logger.LogError(de, "Catching DocumentClientException");

                    if (de.StatusCode != null && (int)de.StatusCode != 429)
                        throw;
                    sleepTime = de.RetryAfter;
                }
                catch (AggregateException ae)
                {
                    _logger.LogError(ae, "Catching AggregateException");

                    if (!(ae.InnerException is DocumentClientException))
                        throw;

                    var de = (DocumentClientException)ae.InnerException;
                    _logger.LogError(de, "InnerException AggregateException");

                    if (de.StatusCode != null && (int)de.StatusCode != 429)
                        throw;
                    sleepTime = de.RetryAfter;
                }

                await Task.Delay(sleepTime);
            }
        }

        private async Task CheckScaling(bool force)
        {
            if (Options.ScaleOfferThroughput || force)
            {

                var client = CreateClient();

                var col = client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(Options.DatabaseId))
                    .Where(x => x.Id == Options.CollectionId)
                    .AsEnumerable()
                    .SingleOrDefault()
                    ;

                var offer =
                client.CreateOfferQuery()
                        .Where(r => r.ResourceLink == col.SelfLink)
                        .AsEnumerable()
                        .SingleOrDefault();

                _logger.LogInformation("Setting OfferThroughput to {0}", Options.OfferThroughput);
                // Set the throughput to the new value, for example 12,000 request units per second
                offer = new OfferV2(offer, Options.OfferThroughput);

                // Now persist these changes to the collection by replacing the original offer resource
                await client.ReplaceOfferAsync(offer);

            }
        }



        private void InitClient()
        {
            Client = new Lazy<DocumentClient>(() =>
            {
                var client = CreateClient();
                client.OpenAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                if (!CheckDatabaseExists())
                {
                    CreateDatabaseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                }

                if (!CheckCollectionExists())
                {
                    CreateCollectionAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                    CheckScaling(true).ConfigureAwait(false).GetAwaiter().GetResult();
                }

                CheckScaling(false).ConfigureAwait(false).GetAwaiter().GetResult();

                return client;
            });
        }


        #region Private

        /// <summary>
        ///     Der Client
        /// </summary>
        public Lazy<DocumentClient> Client { get; private set; }

        /// <summary>
        ///     Obtains the link of a collection
        /// </summary>
        /// <returns></returns>
        private Uri GetCollectionLink()
        {
            return UriFactory.CreateDocumentCollectionUri(Options.DatabaseId, Options.CollectionId);
        }

        /// <summary>
        ///     Obtains the link for a document
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public Uri GetDocumentLink(string id)
        {
            return UriFactory.CreateDocumentUri(Options.DatabaseId, Options.CollectionId, id);
        }

        #endregion

        #region Public

        /// <summary>
        ///     Creates a Query with FeedOptions
        /// </summary>
        /// <typeparam name="T">EntityType of Class to serialize</typeparam>
        /// <param name="feedOptions"></param>
        /// <returns></returns>
        public IQueryable<T> CreateQuery<T>(FeedOptions feedOptions)
        {
            return Client.Value.CreateDocumentQuery<T>(CollectionUri, feedOptions);
        }

        public IQueryable<dynamic> CreateSqlQuery(SqlQuerySpec sqlQuery)
        {
            return Client.Value.CreateDocumentQuery(CollectionUri, sqlQuery);
        }

        //        /// <summary>
        //        /// Creates a Query with FeedOptions and a SQL expression
        //        /// </summary>
        //        /// <typeparam name="T">EntityType of Class to serialize</typeparam>
        //        /// <param name="sqlExpression">SQL query</param>
        //        /// <param name="feedOptions"></param>
        //        /// <returns></returns>
        //        public IQueryable<T> CreateQuery<T>(string sqlExpression, FeedOptions feedOptions)
        //        {
        //            return Client.Value.CreateDocumentQuery<T>(_collectionUri, sqlExpression, feedOptions);
        //        }

        /// <summary>
        ///     Adds an item to a collection
        /// </summary>
        /// <typeparam name="T">EntityType of Class to serialize</typeparam>
        /// <param name="document">Document to add</param>
        /// <returns></returns>
        public async Task<string> AddItemAsync<T>(T document)
        {
            //var result = await Client.Value.CreateDocumentAsync(_collectionUri, document).ConfigureAwait(false);

            var result = await ExecuteWithRetries(() => Client.Value.CreateDocumentAsync(CollectionUri, document));

            _logger.LogTrace("CreateDocumentAsync: StatusCode: {StatusCode} / RequestUnits: {RequestCharge} ",
                result.StatusCode, result.RequestCharge);

            return result.Resource.Id;
        }

        public async Task DeleteItemAsync(string id, string partitionKey)
        {
            //var result = await Client.Value.DeleteDocumentAsync(
            //    GetDocumentLink(id),
            //    new RequestOptions { PartitionKey = new PartitionKey(partitionKey) });

            var result = await ExecuteWithRetries(() => Client.Value.DeleteDocumentAsync(
                 GetDocumentLink(id),
                 new RequestOptions { PartitionKey = new PartitionKey(partitionKey) }));

            _logger.LogTrace("DeleteDocumentAsync: StatusCode: {DocumentId} {StatusCode} / RequestUnits: {RequestCharge} ", id,
                result.StatusCode, result.RequestCharge);
        }

        /// <summary>
        ///     Updates a document on a collection
        /// </summary>
        /// <typeparam name="T">EntityType of Class to serialize</typeparam>
        /// <param name="document">Document to add</param>
        /// <param name="id">EntityId of the document to update</param>
        /// <returns></returns>
        public async Task UpdateItemAsync<T>(T document, string id)
        {
            //var result = await Client.Value.ReplaceDocumentAsync(GetDocumentLink(id), document);
            var result =
                await ExecuteWithRetries(() => Client.Value.ReplaceDocumentAsync(GetDocumentLink(id), document));

            _logger.LogTrace("ReplaceDocumentAsync: {DocumentId} StatusCode: {StatusCode} / RequestUnits: {RequestCharge} ", id,
                result.StatusCode, result.RequestCharge);
        }

        public DocumentCollection GetCollection()
        {
            using (var client = CreateClient())
            {
                var collection = client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(Options.DatabaseId))
                    .Where(c => c.Id == Options.CollectionId)
                    .ToArray()
                    .SingleOrDefault();

                return collection;
            }
        }


        /// <summary>
        ///     prüft ob doc existiert
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<bool> ExistsDocument<T>(string id) where T : Entity
        {
            var source = CreateQuery<T>(new FeedOptions { MaxItemCount = 1, EnableCrossPartitionQuery = false })
                .Where(x => x.EntityId == id && x.EntityType == KeyCache.GetEntityTypeKey<T>());

            var query = source.AsDocumentQuery();
            if (query.HasMoreResults)
            {
                var result = await query.ExecuteNextAsync().ConfigureAwait(false);
                _logger.LogTrace("ExistsDocument: {DocumentId} RequestUnits: {RequestCharge} ", id, result.RequestCharge);

                if (result.Any())
                    return true;
            }

            return false;
        }

        #endregion
    }
}