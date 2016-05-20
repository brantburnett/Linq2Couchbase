using System.Linq;
using Couchbase.Core;
using Couchbase.Linq.Extensions;
using Couchbase.Linq.UnitTests.Documents;
using Moq;
using NUnit.Framework;

namespace Couchbase.Linq.UnitTests.QueryGeneration
{
    [TestFixture]
    public class SelectTests : N1QLTestBase
    {
        [Test]
        public void Test_Select_With_Projection()
        {
            var mockBucket = new Mock<IBucket>();
            mockBucket.SetupGet(e => e.Name).Returns("default");

            var query =
                QueryFactory.Queryable<Contact>(mockBucket.Object)
                    .Select(e => new {age = e.Age, name = e.FirstName});

            const string expected = "SELECT `Extent1`.`age` as `age`, `Extent1`.`fname` as `name` FROM `default` as `Extent1`";

            var n1QlQuery = CreateN1QlQuery(mockBucket.Object, query.Expression);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_WithStronglyTypedProjection()
        {
            var mockBucket = new Mock<IBucket>();
            mockBucket.SetupGet(e => e.Name).Returns("default");

            var query =
                QueryFactory.Queryable<Contact>(mockBucket.Object)
                    .Select(e => new Contact() { Age = e.Age, FirstName = e.FirstName });

            const string expected = "SELECT `Extent1`.`age` as `age`, `Extent1`.`fname` as `fname` FROM `default` as `Extent1`";

            var n1QlQuery = CreateN1QlQuery(mockBucket.Object, query.Expression);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_All_Properties()
        {
            var mockBucket = new Mock<IBucket>();
            mockBucket.SetupGet(e => e.Name).Returns("default");

            var query =
                QueryFactory.Queryable<Contact>(mockBucket.Object)
                    .Select(e => e);

            const string expected = "SELECT `Extent1`.* FROM `default` as `Extent1`";

            var n1QlQuery = CreateN1QlQuery(mockBucket.Object, query.Expression);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_UseKeys()
        {
            var mockBucket = new Mock<IBucket>();
            mockBucket.SetupGet(e => e.Name).Returns("default");

            var query =
                QueryFactory.Queryable<Contact>(mockBucket.Object)
                    .UseKeys(new[] { "abc", "def" })
                    .Select(e => e);

            const string expected = "SELECT `Extent1`.* FROM `default` as `Extent1` USE KEYS ['abc', 'def']";

            var n1QlQuery = CreateN1QlQuery(mockBucket.Object, query.Expression);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_TwoProjections_FirstTyped()
        {
            // This test represents behavior that might be seen using OData
            // Where the model in the bucket is projected, filtered, sorted, and limited
            // Followed by an additional projection from a $select clause

            var mockBucket = new Mock<IBucket>();
            mockBucket.SetupGet(e => e.Name).Returns("default");

            var query =
                QueryFactory.Queryable<Contact>(mockBucket.Object)
                    .Select(e => new ContactModel() {FirstName = e.FirstName, LastName = e.LastName})
                    .OrderBy(e => e.FirstName)
                    .Take(10)
                    .Select(e => new {Key = "1234", Content = new {FullName = e.FirstName + " " + e.LastName}});

            const string expected =
                "SELECT '1234' as `Key`, {\"FullName\": ((`Extent1`.`fname` || ' ') || `Extent1`.`lname`)} as `Content` " +
                "FROM `default` as `Extent1` ORDER BY `Extent1`.`fname` ASC LIMIT 10";

            var n1QlQuery = CreateN1QlQuery(mockBucket.Object, query.Expression);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_TwoProjections_FirstAnonymous()
        {
            // This test represents behavior that might be seen using OData
            // Where the model in the bucket is projected, filtered, sorted, and limited
            // Followed by an additional projection from a $select clause

            var mockBucket = new Mock<IBucket>();
            mockBucket.SetupGet(e => e.Name).Returns("default");

            var query =
                QueryFactory.Queryable<Contact>(mockBucket.Object)
                    .Select(e => new { e.FirstName, e.LastName })
                    .OrderBy(e => e.FirstName)
                    .Take(10)
                    .Select(e => new { Key = "1234", Content = new { FullName = e.FirstName + " " + e.LastName } });

            const string expected =
                "SELECT '1234' as `Key`, {\"FullName\": ((`Extent1`.`fname` || ' ') || `Extent1`.`lname`)} as `Content` " +
                "FROM `default` as `Extent1` ORDER BY `Extent1`.`fname` ASC LIMIT 10";

            var n1QlQuery = CreateN1QlQuery(mockBucket.Object, query.Expression);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_TwoProjections_FirstQuerySource()
        {
            // This test represents behavior that might be seen using OData
            // Where the model in the bucket is projected, filtered, sorted, and limited
            // Followed by an additional projection from a $select clause

            var mockBucket = new Mock<IBucket>();
            mockBucket.SetupGet(e => e.Name).Returns("default");

            var query =
                QueryFactory.Queryable<Contact>(mockBucket.Object)
                    .Select(e => e)
                    .OrderBy(e => e.FirstName)
                    .Take(10)
                    .Select(e => new { Key = "1234", Content = new { FullName = e.FirstName + " " + e.LastName } });

            const string expected =
                "SELECT '1234' as `Key`, {\"FullName\": ((`Extent1`.`fname` || ' ') || `Extent1`.`lname`)} as `Content` " +
                "FROM `default` as `Extent1` ORDER BY `Extent1`.`fname` ASC LIMIT 10";

            var n1QlQuery = CreateN1QlQuery(mockBucket.Object, query.Expression);

            Assert.AreEqual(expected, n1QlQuery);
        }

        [Test]
        public void Test_Select_TwoProjections_NestedObjects()
        {
            // This test represents behavior that might be seen using OData
            // Where the model in the bucket is projected, filtered, sorted, and limited
            // Followed by an additional projection from a $select clause

            var mockBucket = new Mock<IBucket>();
            mockBucket.SetupGet(e => e.Name).Returns("default");

            var query =
                QueryFactory.Queryable<Contact>(mockBucket.Object)
                    .Select(e => new { Name = new { e.FirstName, e.LastName }})
                    .OrderBy(e => e.Name.FirstName)
                    .Take(10)
                    .Select(e => new { Key = "1234", Content = new { FullName = e.Name.FirstName + " " + e.Name.LastName } });

            const string expected =
                "SELECT '1234' as `Key`, {\"FullName\": ((`Extent1`.`fname` || ' ') || `Extent1`.`lname`)} as `Content` " +
                "FROM `default` as `Extent1` ORDER BY `Extent1`.`fname` ASC LIMIT 10";

            var n1QlQuery = CreateN1QlQuery(mockBucket.Object, query.Expression);

            Assert.AreEqual(expected, n1QlQuery);
        }

        #region Helper Classes

        public class ContactModel
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }

        #endregion
    }
}