using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Extensions.Logging;

namespace SoftwarePioniere.ReadModel.Services.AzureCosmosDb
{
    public static class AzureCosmosDbLinqExtensions
    {
        /*
        /// <summary>
        /// Gets the first result
        /// </summary>
        /// <typeparam name="T">EntityType of the Class</typeparam>
        /// <param name="source">Queryable to take one from</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public static T TakeOne<T>(this IQueryable<T> source, CancellationToken token)
        {
            var documentQuery = source.AsDocumentQuery();
            if (documentQuery.HasMoreResults)
            {
                var queryResult = documentQuery.ExecuteNextAsync<T>(token).ConfigureAwait(false).GetAwaiter().GetResult();
                if (queryResult.Any())
                {
                    return queryResult.Single();
                }
            }
            return default(T);
        }
*/

        /*/// <summary>
        /// Gets the first result
        /// </summary>
        /// <typeparam name="T">EntityType of the Class</typeparam>
        /// <param name="source">Queryable to take one from</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public static async Task<T> TakeOneAsync<T>(this IQueryable<T> source, CancellationToken token)
        {
            var documentQuery = source.AsDocumentQuery();
            if (documentQuery.HasMoreResults)
            {
                var queryResult = await documentQuery.ExecuteNextAsync<T>(token).ConfigureAwait(false);
                if (queryResult.Any())
                {
                    return queryResult.Single();
                }
            }
            return default(T);
        }*/


        /*/// <summary>
        /// Creates a pagination wrapper with Continuation Token support
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public static async Task<PagedResults<T>> ToPagedResultsAsync<T>(this IQueryable<T> source, CancellationToken token)
        {
            var documentQuery = source.AsDocumentQuery();
            var results = new PagedResults<T>();

            try
            {
                var queryResult = await documentQuery.ExecuteNextAsync<T>(token).ConfigureAwait(false);
                if (!queryResult.Any())
                {
                    return results;
                }
                results.ContinuationToken = queryResult.ResponseContinuation;

                foreach (var qr in queryResult)
                {
                    results.Results.Add(qr);
                }
                //results.Results.AddRange(queryResult);
            }
            catch
            {
                //documentQuery.ExecuteNextAsync throws an Exception if there are no results
                return results;
            }

            return results;
        }*/


        public static async Task<T[]> ToArrayAsync<T>(this IQueryable<T> source, CancellationToken token, ILogger logger)
        {
            var documentQuery = source.AsDocumentQuery();
            logger.LogTrace("Executing Query : {Query}", source.Expression.ToString());
            List<T> results = new List<T>();
            try
            {


                while (documentQuery.HasMoreResults)
                {
                    var item = await documentQuery.ExecuteNextAsync<T>(token).ConfigureAwait(false);
                    logger.LogTrace("Request Charge for Query {RequestCharge}", item.RequestCharge);
                    results.AddRange(item);
                }

                //var queryResult = await documentQuery.ExecuteNextAsync<T>().ConfigureAwait(false);
                //if (queryResult.Any())
                //{
                //    return queryResult.ToArray();
                //}

            }
            catch
            {
                //documentQuery.ExecuteNextAsync throws an Exception if there are no results
                return null;
            }

            return results.ToArray();
        }
    }
}
