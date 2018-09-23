using System;
using System.Linq;
using Couchbase.Configuration.Client;
using Couchbase.Core;
using Couchbase.Core.Serialization;
using Couchbase.Core.Version;
using Couchbase.Linq.Analytics;
using Couchbase.Linq.Execution;
using Couchbase.Linq.QueryGeneration.MemberNameResolvers;
using Moq;
using Newtonsoft.Json.Serialization;

namespace Couchbase.Linq.UnitTests
{
    public abstract class AnalyticsTestBase
    {
        private const string DefaultBucketName = "default";
        protected static readonly ClusterVersion DefaultClusterVersion = new ClusterVersion(new Version(6, 0, 0));

        internal IMemberNameResolver MemberNameResolver { get; private set; } =
            new JsonNetMemberNameResolver(new DefaultContractResolver());

        internal AnalyticsQueryExecutorEmulator QueryExecutor { get; set; }

        protected virtual IDataSetNameProvider NameProvider => DataSetNameProvider.Current;

        protected AnalyticsTestBase()
        {
            QueryExecutor = new AnalyticsQueryExecutorEmulator(this, DefaultClusterVersion);
        }

        protected virtual string GetQuery<T>(IQueryable<T> query)
        {
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            query.GetEnumerator();

            return QueryExecutor.Query;
        }

        internal virtual IQueryable<T> CreateQueryable<T>()
        {
            var dataSetName = NameProvider.GetDataSetName(typeof(T));

            return CreateQueryable<T>(dataSetName);
        }

        internal virtual IQueryable<T> CreateQueryable<T>(string dataSetName)
        {
            var serializer = new DefaultSerializer();

            var cluster = new Mock<ICluster>();
            cluster.SetupGet(p => p.Configuration).Returns(new ClientConfiguration
            {
                Serializer = () => serializer
            });

            var bucket = new Mock<IBucket>();
            bucket.SetupGet(p => p.Name).Returns(DefaultBucketName);
            bucket.SetupGet(p => p.Cluster).Returns(cluster.Object);
            bucket.As<ITypeSerializerProvider>()
                .SetupGet(p => p.Serializer).Returns(serializer);

            var mockBucket = new Mock<IBucket>();
            mockBucket.SetupGet(e => e.Name).Returns(DefaultBucketName);

            var bucketContext = new Mock<IBucketContext>();
            bucketContext.SetupGet(p => p.Bucket).Returns(mockBucket.Object);
            bucketContext.SetupGet(p => p.Configuration).Returns(cluster.Object.Configuration);

            return new AnalyticsDataSetQueryable<T>(dataSetName,
                QueryParserHelper.CreateQueryParser(bucketContext.Object), QueryExecutor);
        }

        protected void SetContractResolver(IContractResolver contractResolver)
        {
            MemberNameResolver = new JsonNetMemberNameResolver(contractResolver);
        }
    }
}