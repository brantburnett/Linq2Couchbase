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
    internal class SelectProjectionInitExpressionTransfomer : SelectProjectionExpressionTransfomerBase
    {
        private readonly MemberInitExpression _initExpression;

        /// <summary>
        /// Creates a new SelectProjectionInitExpressionTransfomer.
        /// </summary>
        /// <param name="querySourceReference">QuerySourceReferenceExpression that references an IQuerySource for a subquery.</param>
        /// <param name="initExpression"><see cref="MemberInitExpression"/> which was used to create the subquery select projection.</param>
        public SelectProjectionInitExpressionTransfomer(QuerySourceReferenceExpression querySourceReference, MemberInitExpression initExpression)
            : base(querySourceReference)
        {
            if (initExpression == null)
            {
                throw new ArgumentNullException("initExpression");
            }

            _initExpression = initExpression;
        }

        public override Expression Transform(MemberExpression expression)
        {
            if (expression.Expression.Equals(QuerySourceReference))
            {
                for (var i = 0; i < _initExpression.Bindings.Count; i++)
                {
                    if (_initExpression.Bindings[i].BindingType == MemberBindingType.Assignment)
                    {
                        var assignmentBinding = (MemberAssignment) _initExpression.Bindings[i];
                        if (assignmentBinding.Member == expression.Member)
                        {
                            return assignmentBinding.Expression;
                        }
                    }
                }
            }

            return base.Transform(expression);
        }
    }
}
