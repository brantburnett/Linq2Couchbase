using System;
using System.Linq.Expressions;
using Remotion.Linq.Clauses.Expressions;

namespace Couchbase.Linq.QueryGeneration
{
    internal class AnalyticsExpressionTreeVisitor : N1QlExpressionTreeVisitor
    {
        protected AnalyticsExpressionTreeVisitor(N1QlQueryGenerationContext queryGenerationContext)
            : base(queryGenerationContext)
        {
        }

        public static string GetAnalyticsExpression(Expression expression, N1QlQueryGenerationContext queryGenerationContext)
        {
            var visitor = new AnalyticsExpressionTreeVisitor(queryGenerationContext);
            visitor.Visit(expression);
            return visitor.GetN1QlExpression();
        }

        /// <summary>
        ///     Visits a coalesce expression recursively, building a if_missing_or_null function
        /// </summary>
        protected override Expression VisitCoalesceExpression(BinaryExpression expression)
        {
            Expression.Append("if_missing_or_null(");

            Visit(expression.Left);

            var rightExpression = expression.Right;
            while (rightExpression != null)
            {
                Expression.Append(", ");

                if (rightExpression.NodeType == ExpressionType.Coalesce)
                {
                    var subExpression = (BinaryExpression)rightExpression;
                    Visit(subExpression.Left);

                    rightExpression = subExpression.Right;
                }
                else
                {
                    Visit(rightExpression);
                    rightExpression = null;
                }
            }

            Expression.Append(')');

            return expression;
        }

        protected override Expression VisitSubQuery(SubQueryExpression expression)
        {
            var modelVisitor = new AnalyticsQueryModelVisitor(QueryGenerationContext, true);

            modelVisitor.VisitQueryModel(expression.QueryModel);
            Expression.Append(modelVisitor.GetQuery());

            return expression;
        }
    }
}