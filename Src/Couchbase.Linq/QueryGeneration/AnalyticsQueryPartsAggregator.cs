using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Couchbase.Linq.QueryGeneration.FromParts;
using Couchbase.Logging;

namespace Couchbase.Linq.QueryGeneration
{
    internal class AnalyticsQueryPartsAggregator
    {
        private readonly ILog _log = LogManager.GetLogger<AnalyticsQueryPartsAggregator>();

        public AnalyticsQueryPartsAggregator()
        {
            Extents = new List<ExtentPart>();
            LetParts = new List<N1QlLetQueryPart>();
            WhereParts = new List<string>();
            OrderByParts = new List<string>();
        }

        public string SelectPart { get; set; }
        public List<ExtentPart> Extents { get; set; }
        public List<N1QlLetQueryPart> LetParts { get; set; }
        public List<string> WhereParts { get; set; }
        public List<string> OrderByParts { get; set; }
        public List<string> GroupByParts { get; set; }
        public List<string> HavingParts { get; set; }
        public string LimitPart { get; set; }
        public string OffsetPart { get; set; }
        public string DistinctPart { get; set; }
        public string ExplainPart { get; set; }
        public string WhereAllPart { get; set; }
        /// <summary>
        /// For ANY and ALL type subqueries, subquery output elements
        /// will be mapped to this extent name.
        /// </summary>
        public string SubqueryAnyAllExtentName { get; set; }
        /// <summary>
        /// For Array subqueries, list of functions to wrap the result
        /// </summary>
        public List<string> WrappingFunctions { get; set; }
        /// <summary>
        /// For aggregates, wraps the SelectPart with this function call
        /// </summary>
        public string AggregateFunction { get; set; }
        /// <summary>
        /// UNION statements appended to the end of this query
        /// </summary>
        public List<string> UnionParts { get; set; }

        /// <summary>
        /// Indicates the type of query or subquery being generated
        /// </summary>
        /// <remarks>
        /// Defaults to building a SELECT query
        /// </remarks>
        public N1QlQueryType QueryType { get; set; }

        public void AddWherePart(string format, params object[] args)
        {
            WhereParts.Add(string.Format(format, args));
        }

        public void AddExtent(ExtentPart fromPart)
        {
            Extents.Add(fromPart);
        }

        public void AddLetPart(N1QlLetQueryPart letPart)
        {
            LetParts.Add(letPart);
        }

        public void AddDistinctPart(string value)
        {
            DistinctPart = value;
        }

        /// <summary>
        /// Adds an expression to the comma-delimited list of the GROUP BY clause
        /// </summary>
        public void AddGroupByPart(string value)
        {
            if (GroupByParts == null)
            {
                GroupByParts = new List<string>();
            }

            GroupByParts.Add(value);
        }

        /// <summary>
        /// Adds an expression to the HAVING clause, ANDed with any other expressions
        /// </summary>
        public void AddHavingPart(string value)
        {
            if (HavingParts == null)
            {
                HavingParts = new List<string>();
            }

            HavingParts.Add(value);
        }

        private void ApplyLetParts(StringBuilder sb)
        {
            for (var i = 0; i < LetParts.Count; i++)
            {
                sb.Append(i == 0 ? " LET " : ", ");

                sb.AppendFormat("{0} = {1}", LetParts[i].ItemName, LetParts[i].Value);
            }
        }

        public void AddWrappingFunction(string function)
        {
            if (WrappingFunctions == null)
            {
                WrappingFunctions = new List<string>();
            }

            WrappingFunctions.Add(function);
        }

        public void AddUnionPart(string unionPart)
        {
            if (UnionParts == null)
            {
                UnionParts = new List<string>();
            }

            UnionParts.Add(unionPart);
        }

        /// <summary>
        /// Builds a primary select query
        /// </summary>
        /// <returns>Query string</returns>
        private string BuildSelectQuery()
        {
            var sb = new StringBuilder();

            if (QueryType == N1QlQueryType.Subquery)
            {
                sb.Append('(');
            }
            else if (QueryType == N1QlQueryType.SubqueryAny)
            {
                sb.AppendFormat("ANY {0} IN (", SubqueryAnyAllExtentName);
            }
            else if (QueryType == N1QlQueryType.SubqueryAll)
            {
                sb.AppendFormat("EVERY {0} IN (", SubqueryAnyAllExtentName);
            }

            if (!string.IsNullOrWhiteSpace(ExplainPart))
            {
                sb.Append(ExplainPart);
            }

            sb.Append("SELECT VALUE ");

            if (!string.IsNullOrEmpty(AggregateFunction))
            {
                sb.AppendFormat("{0}({1}{2})",
                    AggregateFunction,
                    !string.IsNullOrWhiteSpace(DistinctPart) ? DistinctPart : string.Empty,
                    SelectPart);
            }
            else
            {
                sb.AppendFormat("{0}{1}",
                    !string.IsNullOrWhiteSpace(DistinctPart) ? DistinctPart : string.Empty,
                    SelectPart);
            }

            if (Extents.Any())
            {
                var mainFrom = Extents.First();
                mainFrom.AppendToStringBuilder(sb);

                foreach (var joinPart in Extents.Skip(1))
                {
                    joinPart.AppendToStringBuilder(sb);
                }
            }

            ApplyLetParts(sb);

            if (WhereParts.Any())
            {
                sb.AppendFormat(" WHERE {0}", String.Join(" AND ", WhereParts));
            }
            if ((GroupByParts != null) && GroupByParts.Any())
            {
                sb.AppendFormat(" GROUP BY {0}", string.Join(", ", GroupByParts));
            }
            if ((HavingParts != null) && HavingParts.Any())
            {
                sb.AppendFormat(" HAVING {0}", string.Join(" AND ", HavingParts));
            }

            if (UnionParts != null)
            {
                sb.Append(string.Join("", UnionParts));
            }

            if (OrderByParts.Any())
            {
                sb.AppendFormat(" ORDER BY {0}", String.Join(", ", OrderByParts));
            }
            if (LimitPart != null)
            {
                sb.Append(LimitPart);
            }
            if (LimitPart != null && OffsetPart != null)
            {
                sb.Append(OffsetPart);
            }

            if (QueryType == N1QlQueryType.Subquery)
            {
                sb.Append(')');
            }
            else if (QueryType == N1QlQueryType.SubqueryAny)
            {
                sb.Append(") SATISFIES true END");
            }
            else if (QueryType == N1QlQueryType.SubqueryAll)
            {
                sb.AppendFormat(") SATISFIES {0} END", WhereAllPart);
            }

            return sb.ToString();
        }

        /// <summary>
        /// Builds a main query to return an Any or All result
        /// </summary>
        /// <returns>Query string</returns>
        private string BuildMainAnyAllQuery()
        {
            var sb = new StringBuilder();

            sb.AppendFormat("SELECT VALUE {0}",
                QueryType == N1QlQueryType.MainQueryAny ? "true" : "false");

            if (Extents.Any())
            {
                var mainFrom = Extents.First();
                mainFrom.AppendToStringBuilder(sb);

                foreach (var joinPart in Extents.Skip(1))
                {
                    joinPart.AppendToStringBuilder(sb);
                }
            }

            ApplyLetParts(sb);

            bool hasWhereClause = false;
            if (WhereParts.Any())
            {
                sb.AppendFormat(" WHERE {0}", String.Join(" AND ", WhereParts));

                hasWhereClause = true;
            }

            if (QueryType == N1QlQueryType.MainQueryAll)
            {
                sb.AppendFormat(" {0} NOT ({1})",
                    hasWhereClause ? "AND" : "WHERE",
                    WhereAllPart);
            }

            sb.Append(" LIMIT 1");

            return sb.ToString();
        }
        private string BuildAggregate()
        {
            return $"{AggregateFunction}({SelectPart})";
        }

        public string BuildN1QlQuery()
        {
            string query;

            switch (QueryType)
            {
                case N1QlQueryType.Select:
                case N1QlQueryType.Subquery:
                case N1QlQueryType.SubqueryAny:
                case N1QlQueryType.SubqueryAll:
                    query = BuildSelectQuery();
                    break;

                case N1QlQueryType.MainQueryAny:
                case N1QlQueryType.MainQueryAll:
                    query = BuildMainAnyAllQuery();
                    break;

                case N1QlQueryType.Aggregate:
                    query = BuildAggregate();
                    break;

                default:
                    throw new InvalidOperationException($"Unsupported N1QlQueryType: {QueryType}");
            }

            _log.Debug(query);
            return query;
        }

        public void AddOffsetPart(string offsetPart, int count)
        {
            OffsetPart = string.Format(offsetPart, count);
        }

        public void AddLimitPart(string limitPart, int count)
        {
            LimitPart = string.Format(limitPart, count);
        }

        public void AddOrderByPart(IEnumerable<string> orderings)
        {
            OrderByParts.Insert(0, string.Join(", ", orderings));
        }
    }
}