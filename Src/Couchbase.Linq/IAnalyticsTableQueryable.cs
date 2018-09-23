using System.Linq;

namespace Couchbase.Linq
{
    /// <summary>
    /// IQueryable sourced from a Couchbase Analytics data set.  Used to provide the data set name to the query generator.
    /// </summary>
    public interface IAnalyticsDataSetQueryable
    {
        /// <summary>
        /// Bucket query is run against
        /// </summary>
        string DataSetName { get; }
    }

    /// <summary>
    /// IQueryable sourced from a Couchbase Analytics data set.  Used to provide the data set name to the query generator.
    /// </summary>
    public interface IAnalyticsDataSetQueryable<out T> : IQueryable<T>, IAnalyticsDataSetQueryable
    {
    }
}
