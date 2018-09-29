using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Couchbase.Linq.Clauses;
using Couchbase.Linq.Operators;
using Couchbase.Linq.QueryGeneration.ExpressionTransformers;
using Couchbase.Linq.QueryGeneration.FromParts;
using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Clauses.ResultOperators;
using Remotion.Linq.Parsing.ExpressionVisitors;
using Remotion.Linq.Parsing.ExpressionVisitors.Transformation;

namespace Couchbase.Linq.QueryGeneration
{
    internal class AnalyticsQueryModelVisitor : QueryModelVisitorBase, IN1QlQueryModelVisitor
    {
        #region Constants

        private enum VisitStatus
        {
            None,

            // Group subqueries are used when clauses are applied after a group operation, such as .Where clauses
            // which are treated as HAVING statements.
            InGroupSubquery,
            AfterGroupSubquery,

            // Union sort subqueries are used if .OrderBy clauses are applied after performing a union
            // The require special handling because the sorting is now on the generic name of the columns being returned
            // Not on the original column names
            InUnionSortSubquery,
            AfterUnionSortSubquery
        }

        #endregion

        private readonly N1QlQueryGenerationContext _queryGenerationContext;
        private readonly AnalyticsQueryPartsAggregator _queryPartsAggregator = new AnalyticsQueryPartsAggregator();

        private readonly bool _isSubQuery;

        /// <summary>
        /// Indicates if an aggregate has been applied, which may change select clause handling
        /// </summary>
        private bool _isAggregated;

        /// <summary>
        /// Tracks special status related to the visiting process, which may alter the behavior as query model
        /// clauses are being visited.  For example, .Where clauses are treating as HAVING statements if
        /// _visitStatus == AfterGroupSubquery.
        /// </summary>
        private VisitStatus _visitStatus = VisitStatus.None;

        /// <summary>
        /// Stores the mappings between expressions outside the group query to the extents inside
        /// </summary>
        private ExpressionTransformerRegistry _groupingExpressionTransformerRegistry;

        public AnalyticsQueryModelVisitor(N1QlQueryGenerationContext queryGenerationContext)
            : this(queryGenerationContext, false)
        {
        }

        /// <exception cref="ArgumentNullException"><paramref name="queryGenerationContext"/> is <see langword="null" />.</exception>
        public AnalyticsQueryModelVisitor(N1QlQueryGenerationContext queryGenerationContext, bool isSubQuery)
        {
            _queryGenerationContext = queryGenerationContext ?? throw new ArgumentNullException(nameof(queryGenerationContext));
            _isSubQuery = isSubQuery;

            if (isSubQuery)
            {
                _queryPartsAggregator.QueryType = N1QlQueryType.Subquery;
            }
        }

        public string GetQuery()
        {
            return _queryPartsAggregator.BuildN1QlQuery();
        }

        /// <exception cref="NotSupportedException">N1QL Requires All Group Joins Have A Matching From Clause Subquery</exception>
        public override void VisitQueryModel(QueryModel queryModel)
        {
            queryModel.MainFromClause.Accept(this, queryModel);
            VisitBodyClauses(queryModel.BodyClauses, queryModel);

            VisitResultOperators(queryModel.ResultOperators, queryModel);

            if ((_visitStatus != VisitStatus.InGroupSubquery) && (_visitStatus != VisitStatus.AfterUnionSortSubquery))
            {
                // Select clause should not be visited for grouping subqueries or for the outer query when sorting unions

                // Select clause must be visited after the from clause and body clauses
                // This ensures that any extents are linked before being referenced in the select statement
                // Select clause must be visited after result operations because Any and All operators
                // May change how we handle the select clause

                queryModel.SelectClause.Accept(this, queryModel);
            }
        }

        /// <exception cref="NotSupportedException">N1Ql Bucket Subqueries Require A UseKeys Call</exception>
        public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
        {
            if (fromClause.FromExpression is ConstantExpression bucketConstantExpression &&
                bucketConstantExpression.Value is IAnalyticsDataSetQueryable dataSetQueryable)
            {
                _queryPartsAggregator.AddExtent(new FromPart(fromClause)
                {
                    Source = N1QlHelpers.EscapeIdentifier(dataSetQueryable.DataSetName),
                    ItemName = GetExtentName(fromClause)
                });
            }
            else if (fromClause.FromExpression.NodeType == ExpressionType.MemberAccess)
            {
                if (!_isSubQuery)
                {
                    throw new NotSupportedException("Member Access In The Main From Clause Is Only Supported In Subqueries");
                }

                _queryPartsAggregator.AddExtent(new FromPart(fromClause)
                {
                    Source = GetAnalyticsExpression((MemberExpression) fromClause.FromExpression),
                    ItemName = GetExtentName(fromClause)
                });
            }
            else if (fromClause.FromExpression is SubQueryExpression expression)
            {
                VisitSubQueryFromClause(fromClause, expression);
            }
            else if (fromClause.FromExpression is QuerySourceReferenceExpression querySourceReferenceExpression)
            {
                if (querySourceReferenceExpression.ReferencedQuerySource is GroupJoinClause)
                {
                    // This is an array subquery against a NEST clause
                    VisitArrayFromClause(fromClause);
                }
                else if (fromClause.FromExpression.Equals(_queryGenerationContext.GroupingQuerySource))
                {
                    // We're performing an aggregate against a group
                    _queryPartsAggregator.QueryType = N1QlQueryType.Aggregate;

                    // Ensure that we use the same extent name as the grouping
                    _queryGenerationContext.ExtentNameProvider.LinkExtents(
                        _queryGenerationContext.GroupingQuerySource.ReferencedQuerySource, fromClause);
                }
                else
                {
                    throw new NotSupportedException("From Clause Is Referencing An Invalid Query Source");
                }
            }
            else if (fromClause.FromExpression is ConstantExpression)
            {
                // From clause for this subquery is a constant array

                VisitArrayFromClause(fromClause);
            }

            base.VisitMainFromClause(fromClause, queryModel);
        }

        private void VisitArrayFromClause(MainFromClause fromClause)
        {
            _queryPartsAggregator.AddExtent(new FromPart(fromClause)
            {
                Source = GetAnalyticsExpression(fromClause.FromExpression),
                ItemName = GetExtentName(fromClause)
            });
        }

        private void VisitSubQueryFromClause(MainFromClause fromClause, SubQueryExpression subQuery)
        {
            if (subQuery.QueryModel.ResultOperators.Any(p => p is GroupResultOperator))
            {
                // We're applying functions like HAVING clauses after grouping

                _visitStatus = VisitStatus.InGroupSubquery;
                _queryGenerationContext.GroupingQuerySource = new QuerySourceReferenceExpression(fromClause);

                VisitQueryModel(subQuery.QueryModel);

                _visitStatus = VisitStatus.AfterGroupSubquery;
            }
            else if (subQuery.QueryModel.ResultOperators.Any(p => p is UnionResultOperator || p is ConcatResultOperator))
            {
                // We're applying ORDER BY clauses after a UNION statement is completed

                _visitStatus = VisitStatus.InUnionSortSubquery;

                VisitQueryModel(subQuery.QueryModel);

                _visitStatus = VisitStatus.AfterUnionSortSubquery;

                // When visiting the order by clauses after a union, member references shouldn't include extent names.
                // Instead, they should reference the name of the columns without an extent qualifier.
                _queryGenerationContext.ExtentNameProvider.SetBlankExtentName(fromClause);
            }
            else
            {
                throw new NotSupportedException("Subqueries In The Main From Clause Are Only Supported For Grouping And Unions");
            }
        }

        public virtual void VisitUseKeysClause(UseKeysClause clause, QueryModel queryModel, int index)
        {
            _queryPartsAggregator.AddWherePart(
                "META({0}).id IN {1}",
                _queryGenerationContext.ExtentNameProvider.GetExtentName(queryModel.MainFromClause),
                GetAnalyticsExpression(clause.Keys));
        }

        public virtual void VisitHintClause(HintClause clause, QueryModel queryModel, int index)
        {
            VisitHintClause(clause, _queryPartsAggregator.Extents[0]);
        }

        public virtual void VisitHintClause(HintClause clause, ExtentPart fromPart)
        {
            if (fromPart.Hints == null)
            {
                fromPart.Hints = new List<HintClause>();
            }
            else if (fromPart.Hints.Any(p => p.GetType() == clause.GetType()))
            {
                throw new NotSupportedException($"Only one {clause.GetType().Name} is allowed per extent.");
            }

            fromPart.Hints.Add(clause);
        }

        public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel)
        {
            if (_queryPartsAggregator.QueryType == N1QlQueryType.SubqueryAny)
            {
                // For Any type subqueries, the select statement is unused
                // So just put the extent

                _queryPartsAggregator.SelectPart =
                    _queryGenerationContext.ExtentNameProvider.GetExtentName(queryModel.MainFromClause);
            }
            else if (_queryPartsAggregator.QueryType == N1QlQueryType.SubqueryAll)
            {
                // For All type subqueries, the select statement should just provide all extents
                // So they can be referenced by the SATISFIES statement
                // Select statement that was defined originally is unused

                _queryPartsAggregator.SelectPart = GetExtentSelectParameters();
            }
            else
            {
                _queryPartsAggregator.SelectPart = GetSelectParameters(selectClause);

                base.VisitSelectClause(selectClause, queryModel);
            }
        }

        /// <summary>
        /// Builds select clause when we're directly referencing elements of a query extent.
        /// Could represent an array of documents or an array subdocument.
        /// </summary>
        private string GetQuerySourceSelectParameters(SelectClause selectClause)
        {
            if (_isAggregated)
            {
                return "*";
            }

            return GetAnalyticsExpression(selectClause.Selector);
        }

        private string GetSelectParameters(SelectClause selectClause)
        {
            string expression;

            if (selectClause.Selector is QuerySourceReferenceExpression)
            {
                expression = GetQuerySourceSelectParameters(selectClause);
            }
            else if (selectClause.Selector.NodeType == ExpressionType.New || selectClause.Selector.NodeType == ExpressionType.MemberInit)
            {
                var selector = selectClause.Selector;

                if (_visitStatus == VisitStatus.AfterGroupSubquery)
                {
                    // SELECT clauses must be remapped to refer directly to the extents in the grouping subquery
                    // rather than referring to the output of the grouping subquery

                    selector = TransformingExpressionVisitor.Transform(selector, _groupingExpressionTransformerRegistry);
                }

                expression = GetAnalyticsExpression(selector);
            }
            else
            {
                expression = GetAnalyticsExpression(selectClause.Selector);
            }

            return expression;
        }

        /// <summary>
        /// Provide a SELECT clause to returns all extents from the query
        /// </summary>
        /// <returns></returns>
        private string GetExtentSelectParameters()
        {
            IEnumerable<string> extents = _queryPartsAggregator.Extents.Select(p => p.ItemName);

            if (_queryPartsAggregator.LetParts != null)
            {
                extents = extents.Concat(_queryPartsAggregator.LetParts.Select(p => p.ItemName));
            }
            return string.Join(", ", extents);
        }

        public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
        {
            if (_visitStatus != VisitStatus.AfterGroupSubquery)
            {
                var predicate = whereClause.Predicate;

                if (_queryPartsAggregator.Extents.Count > 1)
                {
                    // There is more than one extent, so one may be an INNER NEST
                    var innerNestDetectingVisitor = new InnerNestDetectingExpressionVisitor(_queryPartsAggregator.Extents);
                    predicate = innerNestDetectingVisitor.Visit(predicate);
                }

                _queryPartsAggregator.AddWherePart(GetAnalyticsExpression(predicate));
            }
            else
            {
                _queryPartsAggregator.AddHavingPart(GetAnalyticsExpression(whereClause.Predicate));
            }

            base.VisitWhereClause(whereClause, queryModel, index);
        }

        public override void VisitResultOperator(ResultOperatorBase resultOperator, QueryModel queryModel, int index)
        {
            if ((resultOperator is TakeResultOperator takeResultOperator))
            {
                _queryPartsAggregator.AddLimitPart(" LIMIT {0}",
                    Convert.ToInt32(GetAnalyticsExpression(takeResultOperator.Count)));
            }
            else if (resultOperator is SkipResultOperator skipResultOperator)
            {
                _queryPartsAggregator.AddOffsetPart(" OFFSET {0}",
                    Convert.ToInt32(GetAnalyticsExpression(skipResultOperator.Count)));
            }
            else if (resultOperator is FirstResultOperator)
            {
                // We can save query execution time with a short circuit for .First()

                _queryPartsAggregator.AddLimitPart(" LIMIT {0}", 1);
            }
            else if (resultOperator is SingleResultOperator)
            {
                // We can save query execution time with a short circuit for .Single()
                // But we have to get at least 2 results so we know if there was more than 1

                _queryPartsAggregator.AddLimitPart(" LIMIT {0}", 2);
            }
            else if (resultOperator is DistinctResultOperator)
            {
                _queryPartsAggregator.AddDistinctPart("DISTINCT ");
            }
            else if (resultOperator is ExplainResultOperator)
            {
                _queryPartsAggregator.ExplainPart = "EXPLAIN ";
            }
            else if (resultOperator is ToQueryRequestResultOperator)
            {
                // Do nothing, conversion will be handled by BucketQueryExecutor
            }
            else if (resultOperator is AnyResultOperator)
            {
                _queryPartsAggregator.QueryType =
                    _queryPartsAggregator.QueryType == N1QlQueryType.Subquery ? N1QlQueryType.SubqueryAny : N1QlQueryType.MainQueryAny;

                if (_queryPartsAggregator.QueryType == N1QlQueryType.SubqueryAny)
                {
                    // For any Any query this value won't be used
                    // But we'll generate it for consistency

                    _queryPartsAggregator.SubqueryAnyAllExtentName =
                        _queryGenerationContext.ExtentNameProvider.GetUnlinkedExtentName();
                }
            }
            else if (resultOperator is AllResultOperator allResultOperator)
            {
                _queryPartsAggregator.QueryType =
                    _queryPartsAggregator.QueryType == N1QlQueryType.Subquery ? N1QlQueryType.SubqueryAll : N1QlQueryType.MainQueryAll;

                bool prefixedExtents = false;
                if (_queryPartsAggregator.QueryType == N1QlQueryType.SubqueryAll)
                {
                    // We're putting allResultOperator.Predicate in the SATISFIES clause of an ALL clause
                    // Each extent of the subquery will be a property returned by the subquery
                    // So we need to prefix the references to the subquery in the predicate with the iterator name from the ALL clause

                    _queryPartsAggregator.SubqueryAnyAllExtentName =
                        _queryGenerationContext.ExtentNameProvider.GetUnlinkedExtentName();

                    prefixedExtents = true;
                    _queryGenerationContext.ExtentNameProvider.Prefix = _queryPartsAggregator.SubqueryAnyAllExtentName + ".";
                }

                _queryPartsAggregator.WhereAllPart = GetAnalyticsExpression(allResultOperator.Predicate);

                if (prefixedExtents)
                {
                    _queryGenerationContext.ExtentNameProvider.Prefix = null;
                }
            }
            else if (resultOperator is ContainsResultOperator containsResultOperator)
            {
                // Use a wrapping function to wrap the subquery with an IN statement

                _queryPartsAggregator.AddWrappingFunction(GetAnalyticsExpression(containsResultOperator.Item) + " IN ");
            }
            else if (resultOperator is GroupResultOperator groupResultOperator)
            {
                VisitGroupResultOperator(groupResultOperator, queryModel);
            }
            else if (resultOperator is AverageResultOperator)
            {
                _queryPartsAggregator.AggregateFunction = "AVG";
                _isAggregated = true;
            }
            else if (resultOperator is CountResultOperator || resultOperator is LongCountResultOperator)
            {
                _queryPartsAggregator.AggregateFunction = "COUNT";
                _isAggregated = true;
            }
            else if (resultOperator is MaxResultOperator)
            {
                _queryPartsAggregator.AggregateFunction = "MAX";
                _isAggregated = true;
            }
            else if (resultOperator is MinResultOperator)
            {
                _queryPartsAggregator.AggregateFunction = "MIN";
                _isAggregated = true;
            }
            else if (resultOperator is SumResultOperator)
            {
                _queryPartsAggregator.AggregateFunction = "SUM";
                _isAggregated = true;
            }
            else if (resultOperator is UnionResultOperator unionResultOperator)
            {
                if (!(unionResultOperator.Source2 is SubQueryExpression source))
                {
                    throw new NotSupportedException("Union is only support against query sources.");
                }

                VisitUnion(source, true);
            }
            else if (resultOperator is ConcatResultOperator concatResultOperator)
            {
                if (!(concatResultOperator.Source2 is SubQueryExpression source))
                {
                    throw new NotSupportedException("Concat is only support against query sources.");
                }

                VisitUnion(source, false);
            }
            else
            {
                throw new NotSupportedException($"{resultOperator.GetType().Name} is not supported.");
            }

            base.VisitResultOperator(resultOperator, queryModel, index);
        }

        private void VisitUnion(SubQueryExpression source, bool distinct)
        {
            var queryModelVisitor = new N1QlQueryModelVisitor(_queryGenerationContext.CloneForUnion());

            queryModelVisitor.VisitQueryModel(source.QueryModel);
            var unionQuery = queryModelVisitor.GetQuery();

            _queryPartsAggregator.AddUnionPart((distinct ? " UNION " : " UNION ALL ") + unionQuery);
        }

        #region Grouping

        protected virtual void VisitGroupResultOperator(GroupResultOperator groupResultOperator, QueryModel queryModel)
        {
            _groupingExpressionTransformerRegistry = new ExpressionTransformerRegistry();

            // Add GROUP BY clause for the grouping key
            // And add transformations for any references to the key

            if (groupResultOperator.KeySelector.NodeType == ExpressionType.New)
            {
                // Grouping by a multipart key, so add each key to the GROUP BY clause

                var newExpression = (NewExpression) groupResultOperator.KeySelector;

                foreach (var argument in newExpression.Arguments)                {
                    _queryPartsAggregator.AddGroupByPart(GetAnalyticsExpression(argument));
                }

                // Use MultiKeyExpressionTransformer to remap access to the Key property

                _groupingExpressionTransformerRegistry.Register(
                    new MultiKeyExpressionTransfomer(_queryGenerationContext.GroupingQuerySource, newExpression));
            }
            else
            {
                // Grouping by a single column

                _queryPartsAggregator.AddGroupByPart(GetAnalyticsExpression(groupResultOperator.KeySelector));

                // Use KeyExpressionTransformer to remap access to the Key property

                _groupingExpressionTransformerRegistry.Register(
                    new KeyExpressionTransfomer(_queryGenerationContext.GroupingQuerySource, groupResultOperator.KeySelector));
            }

            // Add transformations for any references to the element selector

            if (groupResultOperator.ElementSelector is QuerySourceReferenceExpression querySource)
            {
                _queryGenerationContext.ExtentNameProvider.LinkExtents(
                    querySource.ReferencedQuerySource,
                    _queryGenerationContext.GroupingQuerySource.ReferencedQuerySource);
            }
            else
            {
                throw new NotSupportedException("Unsupported GroupResultOperator ElementSelector Type");
            }
        }

        #endregion

        #region Order By Clauses

        public override void VisitOrderByClause(OrderByClause orderByClause, QueryModel queryModel, int index)
        {
            if (_visitStatus == VisitStatus.InGroupSubquery)
            {
                // Just ignore sorting before grouping takes place
                return;
            }

            var orderByParts =
                orderByClause.Orderings.Select(
                    ordering =>
                        string.Concat(GetAnalyticsExpression(ordering.Expression), " ",
                            ordering.OrderingDirection.ToString().ToUpper())).ToList();

            _queryPartsAggregator.AddOrderByPart(orderByParts);

            base.VisitOrderByClause(orderByClause, queryModel, index);
        }

        #endregion

        #region Additional From Clauses

        public override void VisitAdditionalFromClause(AdditionalFromClause fromClause, QueryModel queryModel, int index)
        {
            var handled = false;

            if (fromClause.FromExpression.NodeType == ExpressionType.MemberAccess)
            {
                // Unnest operation

                var fromPart = VisitMemberFromExpression(fromClause, fromClause.FromExpression as MemberExpression);
                _queryPartsAggregator.AddExtent(fromPart);
                handled = true;
            }
            else if (fromClause.FromExpression is SubQueryExpression expression)
            {
                // Might be an unnest or a join to another bucket

                handled = VisitSubQueryFromExpression(fromClause, expression);
            }

            if (!handled)
            {
                throw new NotSupportedException("N1QL Does Not Support This Type Of From Clause");
            }

            base.VisitAdditionalFromClause(fromClause, queryModel, index);
        }

        /// <summary>
        /// Visits an AdditionalFromClause that is executing a subquery
        /// </summary>
        /// <param name="fromClause">AdditionalFromClause being visited</param>
        /// <param name="subQuery">Subquery being executed by the AdditionalFromClause</param>
        /// <returns>True if handled</returns>
        private bool VisitSubQueryFromExpression(AdditionalFromClause fromClause, SubQueryExpression subQuery)
        {
            var mainFromExpression = subQuery.QueryModel.MainFromClause.FromExpression;

            if (mainFromExpression is QuerySourceReferenceExpression expression)
            {
                // Joining to another bucket using a previous group join operation

                return VisitSubQuerySourceReferenceExpression(fromClause, subQuery,
                    expression);
            }
            else if (mainFromExpression.NodeType == ExpressionType.MemberAccess)
            {
                // Unnest operation

                var fromPart = VisitMemberFromExpression(fromClause, mainFromExpression as MemberExpression);

                if (subQuery.QueryModel.ResultOperators.OfType<DefaultIfEmptyResultOperator>().Any())
                {
                    fromPart.JoinType = JoinTypes.LeftUnnest;
                }

                _queryPartsAggregator.AddExtent(fromPart);

                // be sure the subquery clauses use the same extent name
                _queryGenerationContext.ExtentNameProvider.LinkExtents(fromClause, subQuery.QueryModel.MainFromClause);

                // Apply where filters in the subquery to the main query
                VisitBodyClauses(subQuery.QueryModel.BodyClauses, subQuery.QueryModel);

                return true;
            }

            return false;
        }

        /// <summary>
        /// Visit an AdditionalFromClause referencing a previous group join clause
        /// </summary>
        /// <param name="fromClause">AdditionalFromClause being visited</param>
        /// <param name="subQuery">SubQueryExpression being visited</param>
        /// <param name="querySourceReference">QuerySourceReferenceExpression that is the MainFromClause of the SubQuery</param>
        /// <returns>True if the additional from clause is valid.</returns>
        private bool VisitSubQuerySourceReferenceExpression(AdditionalFromClause fromClause, SubQueryExpression subQuery,
            QuerySourceReferenceExpression querySourceReference)
        {
            var ansiNest = _queryPartsAggregator.Extents.OfType<AnsiJoinPart>()
                .FirstOrDefault(p => p.QuerySource == querySourceReference.ReferencedQuerySource);
            if (ansiNest != null)
            {
                // Convert the ANSI NEST to a JOIN because the additional from clause
                // is flattening the query

                ansiNest.JoinType = subQuery.QueryModel.ResultOperators.OfType<DefaultIfEmptyResultOperator>().Any()
                    ? JoinTypes.LeftJoin
                    : JoinTypes.InnerJoin;

                // Be sure that any reference to the subquery gets the join clause extent name
                _queryGenerationContext.ExtentNameProvider.LinkExtents(ansiNest.QuerySource, fromClause);

                return true;
            }

            return false;
        }

        /// <summary>
        /// Visit an AdditionalFromClause referencing a member
        /// </summary>
        /// <param name="fromClause">AdditionalFromClause being visited</param>
        /// <param name="expression">MemberExpression being referenced</param>
        /// <returns>N1QlFromQueryPart to be added to the QueryPartsAggregator.  JoinType is defaulted to INNER UNNEST.</returns>
        private JoinPart VisitMemberFromExpression(AdditionalFromClause fromClause, MemberExpression expression)
        {
            // This case represents an unnest operation

            return new JoinPart(fromClause)
            {
                Source = GetAnalyticsExpression(expression),
                ItemName = GetExtentName(fromClause),
                JoinType = JoinTypes.InnerUnnest
            };
        }

        #endregion

        #region Join Clauses

        public override void VisitJoinClause(JoinClause joinClause, QueryModel queryModel,
            GroupJoinClause groupJoinClause)
        {
            var fromQueryPart = ParseNestJoinClause(joinClause, groupJoinClause);
            _queryPartsAggregator.AddExtent(fromQueryPart);

            base.VisitJoinClause(joinClause, queryModel, groupJoinClause);
        }

        public override void VisitJoinClause(JoinClause joinClause, QueryModel queryModel, int index)
        {
            // basic join clause is an INNER JOIN against another bucket

            var fromQueryPart = ParseJoinClause(joinClause);

            _queryPartsAggregator.AddExtent(fromQueryPart);

            base.VisitJoinClause(joinClause, queryModel, index);
        }

        /// <summary>
        /// Visits a join against either a constant expression of IAnalyticsDataSetQueryable,
        /// or a subquery based on an IBuIAnalyticsDataSetQueryable.
        /// </summary>
        /// <param name="joinClause">Join clause being visited</param>
        /// <returns>N1QlFromQueryPart to be added to the QueryPartsAggregator. JoinType is defaulted to INNER JOIN.</returns>
        /// <remarks>The InnerKeySelector must be selecting the N1QlFunctions.Key of the InnerSequence</remarks>
        private JoinPart ParseJoinClause(JoinClause joinClause)
        {
            if (joinClause.InnerSequence.NodeType == ExpressionType.Constant)
            {
                return VisitConstantExpressionJoinClause(joinClause, joinClause.InnerSequence as ConstantExpression);
            }
            else if (joinClause.InnerSequence is SubQueryExpression subQuery)
            {
                if (subQuery.QueryModel.ResultOperators.Any() ||
                    subQuery.QueryModel.MainFromClause.FromExpression.NodeType != ExpressionType.Constant)
                {
                    throw new NotSupportedException("Unsupported Join Inner Sequence");
                }

                // be sure the subquery clauses use the same name
                _queryGenerationContext.ExtentNameProvider.LinkExtents(joinClause,
                    subQuery.QueryModel.MainFromClause);

                var fromPart = VisitConstantExpressionJoinClause(joinClause,
                    subQuery.QueryModel.MainFromClause.FromExpression as ConstantExpression);

                if (fromPart is AnsiJoinPart ansiJoinPart)
                {
                    // If the right hand extent is filtered the predicates must
                    // be part of the ON statement rather than part of the general predicates.
                    // However, we can only do this for ANSI joins.

                    ansiJoinPart.AdditionalInnerPredicates = string.Join(" AND ",
                        subQuery.QueryModel.BodyClauses
                            .OfType<WhereClause>()
                            .Select(p => GetAnalyticsExpression(p.Predicate)));

                    foreach (var hintClause in subQuery.QueryModel.BodyClauses.OfType<HintClause>())
                    {
                        VisitHintClause(hintClause, fromPart);
                    }
                }
                else
                {
                    if (subQuery.QueryModel.BodyClauses.Any(p => !(p is WhereClause)))
                    {
                        throw new NotSupportedException(
                            "Only predicates are allowed on the right-hand extent of a join");
                    }

                    VisitBodyClauses(subQuery.QueryModel.BodyClauses, subQuery.QueryModel);
                }

                return fromPart;
            }
            else
            {
                throw new NotSupportedException("Unsupported Join Inner Sequence");
            }
        }

        /// <summary>
        /// Visits a join against a constant expression, which must be an IAnalyticsDataSetQueryable implementation.
        /// </summary>
        /// <param name="joinClause">Join clause being visited</param>
        /// <param name="constantExpression">Constant expression that is the InnerSequence of the JoinClause</param>
        /// <returns>N1QlFromQueryPart to be added to the QueryPartsAggregator.  JoinType is defaulted to INNER JOIN.</returns>
        /// <remarks>The InnerKeySelector must be selecting the N1QlFunctions.Key of the InnerSequence</remarks>
        private AnsiJoinPart VisitConstantExpressionJoinClause(JoinClause joinClause, ConstantExpression constantExpression)
        {
            string dataSetName = null;

            if (constantExpression != null)
            {
                if (constantExpression.Value is IAnalyticsDataSetQueryable bucketQueryable)
                {
                    dataSetName = bucketQueryable.DataSetName;
                }
            }

            if (dataSetName == null)
            {
                throw new NotSupportedException("N1QL Joins Must Be Against IAnalyticsDataSetQueryable");
            }

            return new AnsiJoinPart(joinClause)
            {
                Source = N1QlHelpers.EscapeIdentifier(dataSetName),
                ItemName = GetExtentName(joinClause),
                OuterKey = GetAnalyticsExpression(joinClause.OuterKeySelector),
                InnerKey = GetAnalyticsExpression(joinClause.InnerKeySelector),
                JoinType = JoinTypes.InnerJoin
            };
        }

        #endregion

        #region Nest Clause

        public void VisitNestClause(NestClause nestClause, QueryModel queryModel, int index)
        {
            _queryPartsAggregator.AddExtent(ParseNestClause(nestClause));
        }

        /// <summary>
        /// Visits a nest against either a constant expression of IAnalyticsDataSetQueryable,
        /// or a subquery based on an IAnalyticsDataSetQueryable.
        /// </summary>
        /// <param name="nestClause">Nest clause being visited</param>
        /// <returns>N1QlFromQueryPart to be added to the QueryPartsAggregator</returns>
        private JoinPart ParseNestClause(NestClause nestClause)
        {
            if (nestClause.InnerSequence is ConstantExpression constantExpression)
            {
                return VisitConstantExpressionNestClause(nestClause, constantExpression);
            }
            else if (nestClause.InnerSequence is SubQueryExpression subQuery)
            {
                if (subQuery.QueryModel.ResultOperators.Any() ||
                    subQuery.QueryModel.MainFromClause.FromExpression.NodeType != ExpressionType.Constant)
                {
                    throw new NotSupportedException("Unsupported Nest Inner Sequence");
                }

                var fromPart = VisitConstantExpressionNestClause(nestClause,
                    subQuery.QueryModel.MainFromClause.FromExpression as ConstantExpression);

                if (fromPart is AnsiJoinPart ansiJoinPart)
                {
                    // Ensure that the extents are linked before processing the where clause
                    // So they have the same name
                    _queryGenerationContext.ExtentNameProvider.LinkExtents(nestClause, subQuery.QueryModel.MainFromClause);

                    ansiJoinPart.AdditionalInnerPredicates = string.Join(" AND ",
                        subQuery.QueryModel.BodyClauses.OfType<WhereClause>()
                            .Select(p => GetAnalyticsExpression(p.Predicate)));
                }
                else
                {
                    // Put any where clauses in the sub query in an ARRAY filtering clause using a LET statement

                    var whereClauseString = string.Join(" AND ",
                        subQuery.QueryModel.BodyClauses.OfType<WhereClause>()
                            .Select(p => GetAnalyticsExpression(p.Predicate)));

                    var letPart = new N1QlLetQueryPart()
                    {
                        ItemName = GetExtentName(nestClause),
                        Value =
                            string.Format("ARRAY {0} FOR {0} IN {1} WHEN {2} END",
                                GetExtentName(subQuery.QueryModel.MainFromClause),
                                fromPart.ItemName,
                                whereClauseString)
                    };

                    _queryPartsAggregator.AddLetPart(letPart);

                    if (!nestClause.IsLeftOuterNest)
                    {
                        // This is an INNER NEST, but the inner sequence filter is being applied after the NEST operation is done
                        // So we need to put an additional filter to drop rows with an empty array result

                        _queryPartsAggregator.AddWherePart("(ARRAY_LENGTH({0}) > 0)", letPart.ItemName);
                    }
                }

                return fromPart;
            }
            else
            {
                throw new NotSupportedException("Unsupported Nest Inner Sequence");
            }
        }

        /// <summary>
        /// Visits a nest against a constant expression, which must be an IAnalyticsDataSetQueryable implementation.
        /// </summary>
        /// <param name="nestClause">Nest clause being visited</param>
        /// <param name="constantExpression">Constant expression that is the InnerSequence of the NestClause</param>
        /// <returns>N1QlFromQueryPart to be added to the QueryPartsAggregator</returns>
        private JoinPart VisitConstantExpressionNestClause(NestClause nestClause, ConstantExpression constantExpression)
        {
            string dataSetName = null;

            if (constantExpression != null)
            {
                if (constantExpression.Value is IAnalyticsDataSetQueryable bucketQueryable)
                {
                    dataSetName = bucketQueryable.DataSetName;
                }
            }

            if (dataSetName == null)
            {
                throw new NotSupportedException("N1QL Nests Must Be Against IAnalyticsDataSetQueryable");
            }

            var itemName = _queryGenerationContext.ExtentNameProvider.GetExtentName(nestClause);

            return new AnsiJoinPart(nestClause)
            {
                Source = N1QlHelpers.EscapeIdentifier(dataSetName),
                ItemName = itemName,
                JoinType = nestClause.IsLeftOuterNest ? JoinTypes.LeftNest : JoinTypes.InnerNest,
                InnerKey = GetAnalyticsExpression(nestClause.KeySelector),
                OuterKey = $"META({itemName}).id",
                Operator = "IN"
            };
        }

        /// <summary>
        /// Visits an nest join against either a constant expression of IAnalyticsDataSetQueryable,
        /// or a subquery based on an IAnalyticsDataSetQueryable.
        /// </summary>
        /// <param name="joinClause">Join clause being visited</param>
        /// <param name="groupJoinClause">Group join clause being visited</param>
        /// <returns>N1QlFromQueryPart to be added to the QueryPartsAggregator.  JoinType is defaulted to NEST.</returns>
        /// <remarks>The OuterKeySelector must be selecting the N1QlFunctions.Key of the OuterSequence</remarks>
        private JoinPart ParseNestJoinClause(JoinClause joinClause, GroupJoinClause groupJoinClause)
        {
            if (joinClause.InnerSequence.NodeType == ExpressionType.Constant)
            {
                var clause = VisitConstantExpressionJoinClause(joinClause, joinClause.InnerSequence as ConstantExpression);
                clause.JoinType = JoinTypes.LeftNest;
                clause.QuerySource = groupJoinClause;

                _queryGenerationContext.ExtentNameProvider.LinkExtents(joinClause, groupJoinClause);

                return clause;
            }
            else if (joinClause.InnerSequence is SubQueryExpression subQuery)
            {
                if (subQuery.QueryModel.ResultOperators.Any() ||
                    subQuery.QueryModel.MainFromClause.FromExpression.NodeType != ExpressionType.Constant)
                {
                    throw new NotSupportedException("Unsupported Join Inner Sequence");
                }

                // Generate a temporary item name to use on the NEST statement, which we can then reference in the LET statement

                var fromPart = VisitConstantExpressionJoinClause(joinClause,
                    subQuery.QueryModel.MainFromClause.FromExpression as ConstantExpression);
                fromPart.JoinType = JoinTypes.LeftNest;
                fromPart.QuerySource = groupJoinClause;

                // Ensure references to the join pass through to the group join
                _queryGenerationContext.ExtentNameProvider.LinkExtents(joinClause, groupJoinClause);
                _queryGenerationContext.ExtentNameProvider.LinkExtents(joinClause, subQuery.QueryModel.MainFromClause);

                // Put any where clauses in the sub query on the join
                fromPart.AdditionalInnerPredicates = string.Join(" AND ",
                    subQuery.QueryModel.BodyClauses.OfType<WhereClause>()
                        .Select(p => GetAnalyticsExpression(p.Predicate)));

                foreach (var hintClause in subQuery.QueryModel.BodyClauses.OfType<HintClause>())
                {
                    VisitHintClause(hintClause, fromPart);
                }

                return fromPart;
            }
            else
            {
                throw new NotSupportedException("Unsupported Join Inner Sequence");
            }
        }

        #endregion

        private string GetAnalyticsExpression(Expression expression)
        {
            if (_visitStatus == VisitStatus.AfterGroupSubquery)
            {
                // SELECT, HAVING, and ORDER BY clauses must be remapped to refer directly to the extents in the grouping subquery
                // rather than referring to the output of the grouping subquery

                expression = TransformingExpressionVisitor.Transform(expression, _groupingExpressionTransformerRegistry);
            }

            return AnalyticsExpressionTreeVisitor.GetAnalyticsExpression(expression, _queryGenerationContext);
        }

        private string GetExtentName(IQuerySource querySource)
        {
            return _queryGenerationContext.ExtentNameProvider.GetExtentName(querySource);
        }
    }
}