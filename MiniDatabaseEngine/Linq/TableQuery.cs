using System.Collections;
using System.Linq.Expressions;

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
    
    public TableQueryProvider(Table table)
    {
        _table = table ?? throw new ArgumentNullException(nameof(table));
    }
    
    public IQueryable CreateQuery(Expression expression)
    {
        Type elementType = expression.Type.GetGenericArguments()[0];
        try
        {
            return (IQueryable)Activator.CreateInstance(
                typeof(TableQuery<>).MakeGenericType(elementType),
                new object[] { this, expression })!;
        }
        catch (System.Reflection.TargetInvocationException tie)
        {
            throw tie.InnerException!;
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
        var rows = _table.SelectAll();
        
        // Apply Where clauses
        var whereVisitor = new WhereExpressionVisitor();
        whereVisitor.Visit(expression);
        
        foreach (var predicate in whereVisitor.Predicates)
        {
            rows = rows.Where(predicate);
        }
        
        // Apply OrderBy
        var orderVisitor = new OrderByExpressionVisitor();
        orderVisitor.Visit(expression);
        
        if (orderVisitor.OrderByColumn != null)
        {
            if (orderVisitor.IsDescending)
                rows = rows.OrderByDescending(r => r[orderVisitor.OrderByColumn]);
            else
                rows = rows.OrderBy(r => r[orderVisitor.OrderByColumn]);
        }
        
        return (TResult)(object)rows;
    }
}

/// <summary>
/// Visitor to extract WHERE predicates from expression tree
/// </summary>
internal class WhereExpressionVisitor : ExpressionVisitor
{
    public List<Func<DataRow, bool>> Predicates { get; } = new List<Func<DataRow, bool>>();
    
    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.Name == "Where" && node.Method.DeclaringType == typeof(Queryable))
        {
            if (node.Arguments.Count >= 2)
            {
                var lambda = (LambdaExpression)((UnaryExpression)node.Arguments[1]).Operand;
                var compiled = lambda.Compile();
                Predicates.Add((Func<DataRow, bool>)compiled);
            }
        }
        
        return base.VisitMethodCall(node);
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
                
                // Try to extract column name from expression
                if (lambda.Body is MemberExpression member)
                {
                    OrderByColumn = member.Member.Name;
                }
                else if (lambda.Body is MethodCallExpression method && method.Method.Name == "get_Item")
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
}
