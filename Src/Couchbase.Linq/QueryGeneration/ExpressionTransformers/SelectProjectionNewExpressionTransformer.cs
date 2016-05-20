using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Parsing.ExpressionVisitors.Transformation;

namespace Couchbase.Linq.QueryGeneration.ExpressionTransformers
{
    /// <summary>
    /// Transforms references to properties of the select projection of a subquery directly into the expressions that
    /// make up the select projection.  Used to handle select projections against a subquery.
    /// </summary>
    internal class SelectProjectionNewExpressionTransfomer : SelectProjectionExpressionTransfomerBase
    {
        private readonly NewExpression _newExpression;

        /// <summary>
        /// Creates a new SelectProjectionNewExpressionTransfomer.
        /// </summary>
        /// <param name="querySourceReference">QuerySourceReferenceExpression that references an IQuerySource for a subquery.</param>
        /// <param name="newExpression">NewExpression which was used to create the subquery select projection.</param>
        public SelectProjectionNewExpressionTransfomer(QuerySourceReferenceExpression querySourceReference, NewExpression newExpression)
            :base(querySourceReference)
        {
            if (newExpression == null)
            {
                throw new ArgumentNullException("newExpression");
            }

            _newExpression = newExpression;
        }

        public override Expression Transform(MemberExpression expression)
        {
            if (expression.Expression.Equals(QuerySourceReference))
            {
                for (var i = 0; i < _newExpression.Members.Count; i++)
                {
                    if (_newExpression.Members[i] == expression.Member)
                    {
                        return _newExpression.Arguments[i];
                    }
                }
            }

            return base.Transform(expression);
        }
    }
}
