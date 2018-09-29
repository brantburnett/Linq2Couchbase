using System;
using Couchbase.N1QL;
using NUnit.Framework;

namespace Couchbase.Linq.IntegrationTests
{
    [SetUpFixture]
    public class TestSetup : N1QlTestBase
    {
        [OneTimeSetUp]
        public void SetUp()
        {
            ClusterHelper.Initialize(TestConfigurations.DefaultConfig());

            EnsurePrimaryIndexExists(ClusterHelper.GetBucket("beer-sample"));

            PrepareBeerDocuments();
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            ClusterHelper.Close();
        }

        private void PrepareBeerDocuments()
        {
            var bucket = ClusterHelper.GetBucket("beer-sample");
            var query = new QueryRequest(
                @"UPDATE `beer-sample` SET updatedUnixMillis = STR_TO_MILLIS(updated)
                  WHERE type = 'beer' AND updateUnixMillis IS MISSING");

            var result = bucket.Query<dynamic>(query);
            result.EnsureSuccess();
        }
    }
}