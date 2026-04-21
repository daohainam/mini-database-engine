using System.Collections;
using System.Linq.Expressions;
using MiniDatabaseEngine.Storage;

namespace MiniDatabaseEngine.Linq;

/// <summary>
/// IQueryable implementation for table queries
/// </summary>
public class TableQuery<T> : IOrderedQueryable<T>
{
    private readonly TableQueryProvider _provider;
    private readonly Expression _expression;
    
    public TableQuery(Table table)
    {
        _provider = new TableQueryProvider(table);
        _expression = Expression.Constant(this);
    }
    
    public TableQuery(TableQueryProvider provider, Expression expression)
    {
        _provider = provider;
        _expression = expression;
    }
    
    public Type ElementType => typeof(T);
    
    public Expression Expression => _expression;
    
    public IQueryProvider Provider => _provider;
    
    public IEnumerator<T> GetEnumerator()
    {
        return _provider.Execute<IEnumerable<T>>(_expression).GetEnumerator();
    }
    
    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}

/// <summary>
/// Query provider for table queries
/// </summary>
public class TableQueryProvider : IQueryProvider
{
    private readonly Table _table;
    private readonly string _primaryKeyColumn;
    private readonly DataType _primaryKeyType;
    private readonly IComparer<object> _primaryKeyComparer;
    
    public TableQueryProvider(Table table)
    {
        _table = table ?? throw new ArgumentNullException(nameof(table));
        _primaryKeyColumn = _table.Schema.PrimaryKeyColumn;
        _primaryKeyType = ResolvePrimaryKeyType(_table.Schema);
        _primaryKeyComparer = new PrimaryKeyComparer(_primaryKeyType);
    }
    
    public IQueryable CreateQuery(Expression expression)
    {
        if (expression == null)
            throw new ArgumentNullException(nameof(expression));
        
        var genericArgs = expression.Type.GetGenericArguments();
        if (genericArgs.Length == 0)
            throw new ArgumentException("Expression type must be a generic type with at least one type argument", nameof(expression));
        
        Type elementType = genericArgs[0];
        try
        {
            return (IQueryable)Activator.CreateInstance(
                typeof(TableQuery<>).MakeGenericType(elementType),
                new object[] { this, expression })!;
        }
        catch (System.Reflection.TargetInvocationException tie)
        {
            throw tie.InnerException ?? tie;
        }
    }
    
    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        return new TableQuery<TElement>(this, expression);
    }
    
    public object Execute(Expression expression)
    {
        return Execute<IEnumerable<DataRow>>(expression);
    }
    
    public TResult Execute<TResult>(Expression expression)
    {
        var plan = BuildExecutionPlan(expression);
        IEnumerable<DataRow> rows = ExecuteAccessPath(plan);

        foreach (var predicate in plan.Predicates)
        {
            rows = rows.Where(predicate);
        }

        if (plan.OrderByColumn != null)
        {
            if (plan.IsOrderByDescending)
                rows = rows.OrderByDescending(r => r[plan.OrderByColumn]);
            else
                rows = rows.OrderBy(r => r[plan.OrderByColumn]);
        }

        return (TResult)(object)rows;
    }

    private QueryExecutionPlan BuildExecutionPlan(Expression expression)
    {
        var whereVisitor = new WhereExpressionVisitor(_primaryKeyColumn, _primaryKeyType, _primaryKeyComparer);
        whereVisitor.Visit(expression);

        var orderVisitor = new OrderByExpressionVisitor();
        orderVisitor.Visit(expression);

        return new QueryExecutionPlan(
            whereVisitor.Predicates,
            whereVisitor.IndexRange,
            orderVisitor.OrderByColumn,
            orderVisitor.IsDescending);
    }

    private IEnumerable<DataRow> ExecuteAccessPath(QueryExecutionPlan plan)
    {
        if (!string.IsNullOrEmpty(_primaryKeyColumn) && plan.IndexRange != null)
        {
            if (plan.IndexRange.ExactKey != null)
            {
                var single = _table.SelectByKey(plan.IndexRange.ExactKey);
                return single != null ? new List<DataRow> { single } : new List<DataRow>();
            }

            if (plan.IndexRange.HasLowerBound || plan.IndexRange.HasUpperBound)
            {
                return _table.SelectByPrimaryKeyRange(plan.IndexRange.LowerBound, plan.IndexRange.UpperBound);
            }
        }

        return _table.SelectAll();
    }

    private static DataType ResolvePrimaryKeyType(TableSchema schema)
    {
        if (string.IsNullOrEmpty(schema.PrimaryKeyColumn))
            return DataType.Int;

        return schema.Columns.First(c => c.Name == schema.PrimaryKeyColumn).DataType;
    }

    private class PrimaryKeyComparer(DataType dataType) : IComparer<object>
    {
        public int Compare(object? x, object? y) => DataSerializer.Compare(x, y, dataType);
    }
}

/// <summary>
/// Visitor to extract WHERE predicates from expression tree
/// </summary>
internal sealed class QueryExecutionPlan(
    List<Func<DataRow, bool>> predicates,
    IndexRangeConstraint? indexRange,
    string? orderByColumn,
    bool isOrderByDescending)
{
    public List<Func<DataRow, bool>> Predicates { get; } = predicates;
    public IndexRangeConstraint? IndexRange { get; } = indexRange;
    public string? OrderByColumn { get; } = orderByColumn;
    public bool IsOrderByDescending { get; } = isOrderByDescending;
}

internal class WhereExpressionVisitor : ExpressionVisitor
{
    private readonly string _primaryKeyColumn;
    private readonly DataType _primaryKeyType;
    private readonly IComparer<object> _comparer;

    public WhereExpressionVisitor(string primaryKeyColumn, DataType primaryKeyType, IComparer<object> comparer)
    {
        _primaryKeyColumn = primaryKeyColumn;
        _primaryKeyType = primaryKeyType;
        _comparer = comparer;
    }

    public List<Func<DataRow, bool>> Predicates { get; } = new List<Func<DataRow, bool>>();
    public IndexRangeConstraint? IndexRange { get; private set; }
    
    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.Name == "Where" && node.Method.DeclaringType == typeof(Queryable))
        {
            if (node.Arguments.Count >= 2)
            {
                var lambda = (LambdaExpression)((UnaryExpression)node.Arguments[1]).Operand;
                var compiled = lambda.Compile();
                Predicates.Add((Func<DataRow, bool>)compiled);

                if (!string.IsNullOrEmpty(_primaryKeyColumn))
                {
                    var constraint = PrimaryKeyConstraintParser.TryParse(lambda, _primaryKeyColumn, _primaryKeyType);
                    if (constraint != null)
                    {
                        IndexRange = IndexRangeConstraint.Merge(IndexRange, constraint, _comparer);
                    }
                }
            }
        }
        
        return base.VisitMethodCall(node);
    }
}

internal sealed class IndexRangeConstraint
{
    public object? ExactKey { get; private set; }
    public object? LowerBound { get; private set; }
    public bool IsLowerInclusive { get; private set; }
    public object? UpperBound { get; private set; }
    public bool IsUpperInclusive { get; private set; }
    public bool HasLowerBound { get; private set; }
    public bool HasUpperBound { get; private set; }

    public static IndexRangeConstraint Merge(IndexRangeConstraint? current, IndexRangeConstraint next, IComparer<object> comparer)
    {
        var merged = current?.Clone() ?? new IndexRangeConstraint();
        merged.Apply(next, comparer);
        return merged;
    }

    private void Apply(IndexRangeConstraint next, IComparer<object> comparer)
    {
        if (next.ExactKey != null)
        {
            ExactKey = next.ExactKey;
            LowerBound = next.ExactKey;
            UpperBound = next.ExactKey;
            HasLowerBound = true;
            HasUpperBound = true;
            IsLowerInclusive = true;
            IsUpperInclusive = true;
            return;
        }

        if (next.HasLowerBound)
            SetLowerBound(next.LowerBound!, next.IsLowerInclusive, comparer);

        if (next.HasUpperBound)
            SetUpperBound(next.UpperBound!, next.IsUpperInclusive, comparer);
    }

    public void SetExact(object key)
    {
        ExactKey = key;
        LowerBound = key;
        UpperBound = key;
        HasLowerBound = true;
        HasUpperBound = true;
        IsLowerInclusive = true;
        IsUpperInclusive = true;
    }

    public void SetLowerBound(object key, bool inclusive, IComparer<object> comparer)
    {
        if (!HasLowerBound)
        {
            LowerBound = key;
            IsLowerInclusive = inclusive;
            HasLowerBound = true;
            return;
        }

        int compare = comparer.Compare(key, LowerBound!);
        if (compare > 0)
        {
            LowerBound = key;
            IsLowerInclusive = inclusive;
        }
        else if (compare == 0)
        {
            IsLowerInclusive = IsLowerInclusive && inclusive;
        }
    }

    public void SetUpperBound(object key, bool inclusive, IComparer<object> comparer)
    {
        if (!HasUpperBound)
        {
            UpperBound = key;
            IsUpperInclusive = inclusive;
            HasUpperBound = true;
            return;
        }

        int compare = comparer.Compare(key, UpperBound!);
        if (compare < 0)
        {
            UpperBound = key;
            IsUpperInclusive = inclusive;
        }
        else if (compare == 0)
        {
            IsUpperInclusive = IsUpperInclusive && inclusive;
        }
    }

    private IndexRangeConstraint Clone()
    {
        return new IndexRangeConstraint
        {
            ExactKey = ExactKey,
            LowerBound = LowerBound,
            IsLowerInclusive = IsLowerInclusive,
            UpperBound = UpperBound,
            IsUpperInclusive = IsUpperInclusive,
            HasLowerBound = HasLowerBound,
            HasUpperBound = HasUpperBound
        };
    }
}

internal static class PrimaryKeyConstraintParser
{
    public static IndexRangeConstraint? TryParse(LambdaExpression lambda, string primaryKeyColumn, DataType primaryKeyType)
    {
        if (lambda.Parameters.Count != 1)
            return null;

        return TryParseExpression(lambda.Body, lambda.Parameters[0], primaryKeyColumn, primaryKeyType);
    }

    private static IndexRangeConstraint? TryParseExpression(
        Expression expression,
        ParameterExpression rowParameter,
        string primaryKeyColumn,
        DataType primaryKeyType)
    {
        if (expression is BinaryExpression binary && binary.NodeType == ExpressionType.AndAlso)
        {
            var left = TryParseExpression(binary.Left, rowParameter, primaryKeyColumn, primaryKeyType);
            var right = TryParseExpression(binary.Right, rowParameter, primaryKeyColumn, primaryKeyType);

            if (left == null || right == null)
                return null;

            return IndexRangeConstraint.Merge(left, right, new LocalPrimaryKeyComparer(primaryKeyType));
        }

        if (expression is BinaryExpression comparison &&
            IsComparisonOperator(comparison.NodeType) &&
            TryReadComparison(comparison, rowParameter, primaryKeyColumn, primaryKeyType, out var constraint))
        {
            return constraint;
        }

        return null;
    }

    private static bool TryReadComparison(
        BinaryExpression comparison,
        ParameterExpression rowParameter,
        string primaryKeyColumn,
        DataType primaryKeyType,
        out IndexRangeConstraint constraint)
    {
        constraint = new IndexRangeConstraint();

        var left = StripConvert(comparison.Left);
        var right = StripConvert(comparison.Right);

        if (IsPrimaryKeyAccessor(left, rowParameter, primaryKeyColumn) &&
            TryEvaluateValue(right, rowParameter, primaryKeyType, out var rightValue))
        {
            return ApplyConstraint(constraint, comparison.NodeType, rightValue, columnOnLeft: true, primaryKeyType);
        }

        if (IsPrimaryKeyAccessor(right, rowParameter, primaryKeyColumn) &&
            TryEvaluateValue(left, rowParameter, primaryKeyType, out var leftValue))
        {
            return ApplyConstraint(constraint, comparison.NodeType, leftValue, columnOnLeft: false, primaryKeyType);
        }

        return false;
    }

    private static bool ApplyConstraint(
        IndexRangeConstraint constraint,
        ExpressionType comparisonType,
        object value,
        bool columnOnLeft,
        DataType primaryKeyType)
    {
        if (!TryConvertToPrimaryKeyType(value, primaryKeyType, out var typedValue))
            return false;

        var comparer = new LocalPrimaryKeyComparer(primaryKeyType);
        switch (comparisonType)
        {
            case ExpressionType.Equal:
                constraint.SetExact(typedValue);
                return true;
            case ExpressionType.GreaterThan:
                if (columnOnLeft) constraint.SetLowerBound(typedValue, inclusive: false, comparer);
                else constraint.SetUpperBound(typedValue, inclusive: false, comparer);
                return true;
            case ExpressionType.GreaterThanOrEqual:
                if (columnOnLeft) constraint.SetLowerBound(typedValue, inclusive: true, comparer);
                else constraint.SetUpperBound(typedValue, inclusive: true, comparer);
                return true;
            case ExpressionType.LessThan:
                if (columnOnLeft) constraint.SetUpperBound(typedValue, inclusive: false, comparer);
                else constraint.SetLowerBound(typedValue, inclusive: false, comparer);
                return true;
            case ExpressionType.LessThanOrEqual:
                if (columnOnLeft) constraint.SetUpperBound(typedValue, inclusive: true, comparer);
                else constraint.SetLowerBound(typedValue, inclusive: true, comparer);
                return true;
            default:
                return false;
        }
    }

    private static bool IsPrimaryKeyAccessor(Expression expression, ParameterExpression rowParameter, string primaryKeyColumn)
    {
        if (expression is not MethodCallExpression method ||
            method.Method.Name != "get_Item" ||
            method.Object == null ||
            method.Arguments.Count != 1)
        {
            return false;
        }

        if (StripConvert(method.Object) != rowParameter)
            return false;

        if (method.Arguments[0] is not ConstantExpression columnConstant || columnConstant.Value is not string columnName)
            return false;

        return string.Equals(columnName, primaryKeyColumn, StringComparison.Ordinal);
    }

    private static bool TryEvaluateValue(Expression expression, ParameterExpression rowParameter, DataType primaryKeyType, out object value)
    {
        value = string.Empty;
        if (ContainsParameter(expression, rowParameter))
            return false;

        try
        {
            var boxed = Expression.Convert(expression, typeof(object));
            var lambda = Expression.Lambda<Func<object>>(boxed);
            var evaluated = lambda.Compile().Invoke();
            if (evaluated == null)
                return false;

            if (!TryConvertToPrimaryKeyType(evaluated, primaryKeyType, out value))
                return false;

            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryConvertToPrimaryKeyType(object value, DataType primaryKeyType, out object converted)
    {
        converted = value;
        try
        {
            var targetType = GetClrType(primaryKeyType);
            if (value.GetType() == targetType)
            {
                converted = value;
                return true;
            }

            converted = Convert.ChangeType(value, targetType);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static Type GetClrType(DataType dataType)
    {
        return dataType switch
        {
            DataType.Byte => typeof(byte),
            DataType.SByte => typeof(sbyte),
            DataType.Short => typeof(short),
            DataType.UShort => typeof(ushort),
            DataType.Int => typeof(int),
            DataType.UInt => typeof(uint),
            DataType.Long => typeof(long),
            DataType.ULong => typeof(ulong),
            DataType.Bool => typeof(bool),
            DataType.Char => typeof(char),
            DataType.String => typeof(string),
            DataType.Float => typeof(float),
            DataType.Double => typeof(double),
            DataType.Decimal => typeof(decimal),
            DataType.DateTime => typeof(DateTime),
            _ => typeof(object)
        };
    }

    private static bool ContainsParameter(Expression expression, ParameterExpression parameter)
    {
        var visitor = new ParameterReferenceVisitor(parameter);
        visitor.Visit(expression);
        return visitor.ContainsParameter;
    }

    private static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression unary &&
               (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
        {
            expression = unary.Operand;
        }

        return expression;
    }

    private static bool IsComparisonOperator(ExpressionType nodeType)
    {
        return nodeType == ExpressionType.Equal ||
               nodeType == ExpressionType.GreaterThan ||
               nodeType == ExpressionType.GreaterThanOrEqual ||
               nodeType == ExpressionType.LessThan ||
               nodeType == ExpressionType.LessThanOrEqual;
    }

    private sealed class ParameterReferenceVisitor(ParameterExpression parameter) : ExpressionVisitor
    {
        private readonly ParameterExpression _parameter = parameter;
        public bool ContainsParameter { get; private set; }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _parameter)
                ContainsParameter = true;

            return base.VisitParameter(node);
        }
    }

    private sealed class LocalPrimaryKeyComparer(DataType dataType) : IComparer<object>
    {
        public int Compare(object? x, object? y) => DataSerializer.Compare(x, y, dataType);
    }
}

/// <summary>
/// Visitor to extract ORDER BY clauses from expression tree
/// </summary>
internal class OrderByExpressionVisitor : ExpressionVisitor
{
    public string? OrderByColumn { get; private set; }
    public bool IsDescending { get; private set; }
    
    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if ((node.Method.Name == "OrderBy" || node.Method.Name == "OrderByDescending") 
            && node.Method.DeclaringType == typeof(Queryable))
        {
            IsDescending = node.Method.Name == "OrderByDescending";
            
            if (node.Arguments.Count >= 2)
            {
                var lambda = (LambdaExpression)((UnaryExpression)node.Arguments[1]).Operand;
                var body = StripConvert(lambda.Body);
                
                // Try to extract column name from expression
                if (body is MemberExpression member)
                {
                    OrderByColumn = member.Member.Name;
                }
                else if (body is MethodCallExpression method && method.Method.Name == "get_Item")
                {
                    if (method.Arguments[0] is ConstantExpression constant)
                    {
                        OrderByColumn = constant.Value?.ToString();
                    }
                }
            }
        }
        
        return base.VisitMethodCall(node);
    }

    private static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression unary &&
               (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
        {
            expression = unary.Operand;
        }

        return expression;
    }
}
