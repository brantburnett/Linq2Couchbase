using System;
using System.Linq;
using Couchbase.Linq.IntegrationTests.Documents;
using NUnit.Framework;

namespace Couchbase.Linq.IntegrationTests
{
    [TestFixture]
    public class AnalyticsTests : N1QlTestBase
    {
        [Test]
        public void SelectProjectionTests()
        {
            var bucket = ClusterHelper.GetBucket("beer-sample");
            var context = new BucketContext(bucket);

            var beers = from b in context.Analyze<Beer>()
                select b;

            var results = beers.Take(1).ToList();
            Assert.AreEqual(1, results.Count());

            foreach (var beer in results)
            {
                Console.WriteLine(beer.Name);
            }
        }

        [Test]
        public void SelectProjectionTests_Simple_Projections()
        {
            var bucket = ClusterHelper.GetBucket("beer-sample");
            var context = new BucketContext(bucket);

            var beers = from b in context.Analyze<Beer>()
                select new {name = b.Name, abv = b.Abv};

            var results = beers.Take(1).ToList();
            Assert.AreEqual(1, results.Count());

            foreach (var b in results)
            {
                Console.WriteLine("{0} has {1} ABV", b.name, b.abv);
            }
        }
    }
}
