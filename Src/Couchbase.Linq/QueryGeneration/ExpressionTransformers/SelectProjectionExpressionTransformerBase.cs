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
    /// Base class for transformers that transforms references to properties of the select projection of a subquery
    /// directly into the expressions that  make up the select projection.  Used to handle select projections against
    /// a subquery.Implements common logic to flatten member access in the outer select projection that drill into
    /// nested objects in the inner select projection.
    /// </summary>
    internal abstract class SelectProjectionExpressionTransfomerBase : IExpressionTransformer<MemberExpression>
    {
        public ExpressionType[] SupportedExpressionTypes
        {
            get
            {
                return new[]
                {
                    ExpressionType.MemberAccess
                };
            }
        }

        private readonly QuerySourceReferenceExpression _querySourceReference;
        protected QuerySourceReferenceExpression QuerySourceReference
        {
            get { return _querySourceReference; }
        }

        /// <summary>
        /// Creates a new SelectProjectionExpressionTransfomerBase.
        /// </summary>
        /// <param name="querySourceReference">QuerySourceReferenceExpression that references an IQuerySource for a subquery.</param>
        protected SelectProjectionExpressionTransfomerBase(QuerySourceReferenceExpression querySourceReference)
        {
            if (querySourceReference == null)
            {
                throw new ArgumentNullException("querySourceReference");
            }

            _querySourceReference = querySourceReference;
        }

        public virtual Expression Transform(MemberExpression expression)
        {
            var newExpression = expression.Expression as NewExpression;
            if (newExpression != null)
            {
                for (var i = 0; i < newExpression.Members.Count; i++)
                {
                    if (newExpression.Members[i] == expression.Member)
                    {
                        return newExpression.Arguments[i];
                    }
                }
            }

            var initExpression = expression.Expression as MemberInitExpression;
            if (initExpression != null)
            {
                for (var i = 0; i < initExpression.Bindings.Count; i++)
                {
                    if (initExpression.Bindings[i].BindingType == MemberBindingType.Assignment)
                    {
                        var assignmentBinding = (MemberAssignment)initExpression.Bindings[i];
                        if (assignmentBinding.Member == expression.Member)
                        {
                            return assignmentBinding.Expression;
                        }
                    }
                }
            }

            return expression;
        }
    }
}
