using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Couchbase.Analytics;
using Remotion.Linq;

namespace Couchbase.Linq.Execution
{
    /// <summary>
    /// Extends <see cref="IQueryExecutor"/> with routines to execute an analytics query asynchronously.
    /// </summary>
    internal interface IAnalyticsQueryExecutor : IQueryExecutor
    {
        /// <summary>
        /// Asynchronously execute an <see cref="AnalyticsRequest"/>.
        /// </summary>
        /// <typeparam name="T">Type returned by the query.</typeparam>
        /// <param name="queryRequest">Request to execute.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Task which contains a list of objects returned by the request when complete.</returns>
        Task<IEnumerable<T>> ExecuteCollectionAsync<T>(AnalyticsRequest queryRequest, CancellationToken cancellationToken);

        /// <summary>
        /// Asynchronously execute an <see cref="AnalyticsRequest"/> that returns a single result.
        /// </summary>
        /// <typeparam name="T">Type returned by the query.</typeparam>
        /// <param name="queryRequest">Request to execute.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Task which contains the object returned by the request when complete.</returns>
        Task<T> ExecuteSingleAsync<T>(AnalyticsRequest queryRequest, CancellationToken cancellationToken);
    }
}
