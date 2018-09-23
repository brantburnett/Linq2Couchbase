using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Couchbase.Analytics;
using Couchbase.Core.Serialization;
using Couchbase.Linq.QueryGeneration;
using Couchbase.Linq.QueryGeneration.MemberNameResolvers;
using Couchbase.Linq.Utils;
using Couchbase.Linq.Versioning;
using Couchbase.Logging;
using Couchbase.N1QL;
using Remotion.Linq;

namespace Couchbase.Linq.Execution
{
    internal class AnalyticsQueryExecutor : IAnalyticsQueryExecutor
    {
        private readonly ILog _log = LogManager.GetLogger<AnalyticsQueryExecutor>();

        private readonly IBucketContext _bucketContext;
        private ITypeSerializer _serializer;

        private ITypeSerializer Serializer
        {
            get
            {
                if (_serializer == null)
                {
                    var serializerProvider = _bucketContext.Bucket as ITypeSerializerProvider;

                    _serializer = serializerProvider?.Serializer ?? _bucketContext.Configuration.Serializer.Invoke();
                }

                return _serializer;
            }
        }

        /// <summary>
        /// Creates a new BucketQueryExecutor.
        /// </summary>
        /// <param name="bucketContext">The context object for tracking and managing changes to documents.</param>
        public AnalyticsQueryExecutor(IBucketContext bucketContext)
        {
            _bucketContext = bucketContext;
        }

        public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
        {
            var commandData = GenerateQuery(queryModel);

            var queryRequest = new AnalyticsRequest(commandData);

            return ExecuteCollection<T>(queryRequest);
        }

        /// <summary>
        /// Execute a <see cref="LinqQueryRequest"/>.
        /// </summary>
        /// <typeparam name="T">Type returned by the query.</typeparam>
        /// <param name="queryRequest">Request to execute.</param>
        /// <returns>List of objects returned by the request.</returns>
        public IEnumerable<T> ExecuteCollection<T>(AnalyticsRequest queryRequest)
        {
            var result = _bucketContext.Bucket.Query<T>(queryRequest);

            return ParseResult(result);
        }

        /// <summary>
        /// Asynchronously execute a <see cref="LinqQueryRequest"/>.
        /// </summary>
        /// <typeparam name="T">Type returned by the query.</typeparam>
        /// <param name="queryRequest">Request to execute.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Task which contains a list of objects returned by the request when complete.</returns>
        public async Task<IEnumerable<T>> ExecuteCollectionAsync<T>(AnalyticsRequest queryRequest, CancellationToken cancellationToken)
        {
            var result = await _bucketContext.Bucket.QueryAsync<T>(queryRequest, cancellationToken).ConfigureAwait(false);

            return ParseResult(result);
        }

        /// <summary>
        /// Parses a <see cref="IQueryResult{T}"/>, returning the result rows.
        /// If there are any errors, throws exceptions instead.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="result">Result to parse.</param>
        /// <returns>Rows in the result.</returns>
        private IEnumerable<T> ParseResult<T>(IAnalyticsResult<T> result)
        {
            if (!result.Success)
            {
                if (result.Errors != null && result.Errors.Count > 0)
                {
                    var message = result.Errors.Count == 1 ?
                        result.Errors[0].Message :
                        ExceptionMsgs.QueryExecutionMultipleErrors;

                    throw new CouchbaseQueryException(message ?? ExceptionMsgs.QueryExecutionUnknownError, result.Errors);
                }
                else if (result.Exception != null)
                {
                    throw new CouchbaseQueryException(ExceptionMsgs.QueryExecutionException, result.Exception);
                }
                else
                {
                    throw new CouchbaseQueryException(ExceptionMsgs.QueryExecutionUnknownError);
                }
            }

            return result;
        }

        public T ExecuteScalar<T>(QueryModel queryModel)
        {
            return ExecuteSingle<T>(queryModel, false);
        }

        public T ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty)
        {
            var result = returnDefaultWhenEmpty
                ? ExecuteCollection<T>(queryModel).SingleOrDefault()
                : ExecuteCollection<T>(queryModel).Single();

            return result;
        }

        public async Task<T> ExecuteSingleAsync<T>(AnalyticsRequest queryRequest, CancellationToken cancellationToken)
        {
            var resultSet = await ExecuteCollectionAsync<T>(queryRequest, cancellationToken).ConfigureAwait(false);

            return resultSet.Single();
        }

        public string GenerateQuery(QueryModel queryModel)
        {
            // If ITypeSerializer is an IExtendedTypeSerializer, use it as the member name resolver
            // Otherwise fallback to the legacy behavior which assumes we're using Newtonsoft.Json
            // Note that DefaultSerializer implements IExtendedTypeSerializer, but has the same logic as JsonNetMemberNameResolver

            var serializer = Serializer as IExtendedTypeSerializer;

#pragma warning disable CS0618 // Type or member is obsolete
            var memberNameResolver = serializer != null ?
                (IMemberNameResolver)new ExtendedTypeSerializerMemberNameResolver(serializer) :
                (IMemberNameResolver)new JsonNetMemberNameResolver(_bucketContext.Configuration.SerializationSettings.ContractResolver);
#pragma warning restore CS0618 // Type or member is obsolete

            var methodCallTranslatorProvider = new DefaultMethodCallTranslatorProvider();

            var queryGenerationContext = new N1QlQueryGenerationContext
            {
                MemberNameResolver = memberNameResolver,
                MethodCallTranslatorProvider = methodCallTranslatorProvider,
                Serializer = serializer,
                ClusterVersion = VersionProvider.Current.GetVersion(_bucketContext.Bucket)
            };

            var visitor = new AnalyticsQueryModelVisitor(queryGenerationContext);
            visitor.VisitQueryModel(queryModel);

            var query = visitor.GetQuery();
            _log.Debug("Generated query: {0}", query);

            return query;
        }
    }
}