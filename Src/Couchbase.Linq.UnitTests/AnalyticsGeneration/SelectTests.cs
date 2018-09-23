using System;
using System.Linq;
using Couchbase.Core;
using Couchbase.Core.Serialization;
using Couchbase.Linq.Analytics;
using Couchbase.Linq.Extensions;
using Couchbase.Linq.UnitTests.Documents;
using Moq;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Couchbase.Linq.UnitTests.AnalyticsGeneration
{
    [TestFixture]
    public class SelectTests : AnalyticsTestBase
    {
        [Test]
        public void Test_Select_With_Projection()
        {
            var query =
                CreateQueryable<Contact>()
                    .Select(e => new {age = e.Age, name = e.FirstName});

            const string expected = "SELECT VALUE {\"age\": `Extent1`.`age`, \"name\": `Extent1`.`fname`} " +
                                    "FROM `contacts` as `Extent1`";

            var n1QlQuery = GetQuery(query);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_WithStronglyTypedProjection()
        {
            var mockBucket = new Mock<IBucket>();
            mockBucket.SetupGet(e => e.Name).Returns("default");

            var query =
                CreateQueryable<Contact>()
                    .Select(e => new Contact { Age = e.Age, FirstName = e.FirstName });

            const string expected = "SELECT VALUE {\"age\": `Extent1`.`age`, \"fname\": `Extent1`.`fname`} " +
                                    "FROM `contacts` as `Extent1`";

            var n1QlQuery = GetQuery(query);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_WithUnixMillisecondsProjection()
        {
            var mockBucket = new Mock<IBucket>();
            mockBucket.SetupGet(e => e.Name).Returns("default");

            var query =
                CreateQueryable<UnixMillisecondsDocument>()
                    .Select(e => new UnixMillisecondsDocument { DateTime = e.DateTime });

            // Since the source and dest are both using UnixMillisecondsConverter, no functions should be applied
            const string expected = "SELECT VALUE {\"DateTime\": `Extent1`.`DateTime`} FROM `default` as `Extent1`";

            var n1QlQuery = GetQuery(query);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_WithUnixMillisecondsToIsoProjection()
        {
            var mockBucket = new Mock<IBucket>();
            mockBucket.SetupGet(e => e.Name).Returns("default");

            var query =
                CreateQueryable<UnixMillisecondsDocument>()
                    .Select(e => new IsoDocument { DateTime = e.DateTime });

            const string expected = "SELECT VALUE {\"DateTime\": MILLIS_TO_STR(`Extent1`.`DateTime`)} " +
                                    "FROM `default` as `Extent1`";

            var n1QlQuery = GetQuery(query);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_WithIsoToUnixMillisecondsProjection()
        {
            var query =
                CreateQueryable<IsoDocument>("default")
                    .Select(e => new UnixMillisecondsDocument { DateTime = e.DateTime });

            const string expected = "SELECT VALUE {\"DateTime\": STR_TO_MILLIS(`Extent1`.`DateTime`)} " +
                                    "FROM `default` as `Extent1`";

            var n1QlQuery = GetQuery(query);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_All_Properties()
        {
            var query =
                CreateQueryable<Contact>()
                    .Select(e => e);

            const string expected = "SELECT VALUE `Extent1` FROM `contacts` as `Extent1`";

            var n1QlQuery = GetQuery(query);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_Single_Property()
        {
            var query =
                CreateQueryable<Contact>()
                    .Select(e => e.FirstName);

            const string expected = "SELECT VALUE `Extent1`.`fname` FROM `contacts` as `Extent1`";

            var n1QlQuery = GetQuery(query);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_UseKeys()
        {
            var query =
                CreateQueryable<Contact>()
                    .UseKeys(new[] { "abc", "def" })
                    .Select(e => e);

            const string expected = "SELECT VALUE `Extent1` FROM `contacts` as `Extent1` " +
                                    "WHERE META(`Extent1`).id IN ['abc', 'def']";

            var n1QlQuery = GetQuery(query);

            Assert.AreEqual(expected, n1QlQuery);
        }

        #region Helpers

        [DataSet("default")]
        public class IsoDocument
        {
            public DateTime DateTime { get; set; }
        }

        [DataSet("default")]
        public class UnixMillisecondsDocument
        {
            [JsonConverter(typeof(UnixMillisecondsConverter))]
            public DateTime DateTime { get; set; }
        }

        #endregion
    }
}