using System;
using System.Linq;
using System.Linq.Expressions;
using Couchbase.Linq.Execution;
using Remotion.Linq;
using Remotion.Linq.Parsing.Structure;

namespace Couchbase.Linq
{
    /// <summary>
    /// The main entry point and executor of the query.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class AnalyticsDataSetQueryable<T> : QueryableBase<T>, IAnalyticsDataSetQueryable
    {
        /// <summary>
        /// Get the <see cref="IAnalyticsQueryExecutor"/>.
        /// </summary>
        public IAnalyticsQueryExecutor AnalyticsQueryExecutor { get; }

        /// <inheritdoc/>
        public string DataSetName { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="BucketQueryable{T}"/> class.
        /// </summary>
        /// <param name="dataSetName">Name of the data set to query.</param>
        /// <param name="queryParser">The query parser.</param>
        /// <param name="executor">The executor.</param>
        /// <exception cref="System.ArgumentNullException">bucket</exception>
        public AnalyticsDataSetQueryable(string dataSetName, IQueryParser queryParser, IAnalyticsQueryExecutor executor)
            : base(queryParser, executor)
        {
            DataSetName = dataSetName;
            AnalyticsQueryExecutor = executor;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BucketQueryable{T}"/> class.
        /// </summary>
        /// <remarks>Used to build new expressions as more methods are applied to the query.</remarks>
        /// <param name="provider">The provider.</param>
        /// <param name="expression">The expression.</param>
        public AnalyticsDataSetQueryable(IQueryProvider provider, Expression expression)
            : base(provider, expression)
        {
            AnalyticsQueryExecutor = (IAnalyticsQueryExecutor) ((DefaultQueryProvider) provider).Executor;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BucketQueryable{T}"/> class.
        /// </summary>
        /// <param name="dataSetName">Name of the data set being queried.</param>
        /// <param name="bucketContext">The bucket context being queried.</param>
        /// <exception cref="System.ArgumentNullException">bucket</exception>
        public AnalyticsDataSetQueryable(string dataSetName, IBucketContext bucketContext)
            : base(QueryParserHelper.CreateQueryParser(bucketContext), new AnalyticsQueryExecutor(bucketContext))
        {
            DataSetName = dataSetName;
            AnalyticsQueryExecutor = (IAnalyticsQueryExecutor) ((DefaultQueryProvider) Provider).Executor;
        }
    }
}