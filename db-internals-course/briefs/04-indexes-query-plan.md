# Module 4: Indexes & the Query Plan

### Teaching Arc
- **Metaphor:** A **GPS route planner**. When you ask for directions, it doesn't try every possible path — it looks at your request, notices "oh, they said 'highway only'," and picks an efficient access path. LINQ queries like `.Where(r => (int)r["Id"] > 5)` go through a planner that asks: "Is the filter on the indexed column? If yes, use the B+ tree. If not, scan the whole table."
- **Opening hook:** The same LINQ query can be lightning-fast or painfully slow depending on what the planner decides. You wrote `.Where(r => r["Id"] == 42)` — did it hit the index or scan all 10 million rows? The query plan holds the answer.
- **Key insight:** A query is just a **request**. The planner turns the request into a **strategy** (access path + filters + sort). This two-step — *declare what you want* vs. *decide how to get it* — is why SQL (and LINQ) feel magical: you describe the destination, the planner picks the route.
- **"Why should I care?":** "Why is this query slow?" is the most common question from users. 90% of the time the answer is: the planner couldn't use an index. If you can read a query plan, you can fix most slowness yourself instead of asking AI for guesses.

### Code Snippets (pre-extracted)

**File: MiniDatabaseEngine/Table.cs (lines 226-241) — point lookup by key**
```csharp
public DataRow? SelectByKey(object key)
{
    _lock.EnterReadLock();
    try
    {
        var serialized = _index.Search(key);
        if (serialized == null)
            return null;
        
        return DeserializeRow((byte[])serialized);
    }
    finally
    {
        _lock.ExitReadLock();
    }
}
```

**File: MiniDatabaseEngine/Linq/TableQuery.cs (lines 94-113) — Execute**
```csharp
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
```

**File: MiniDatabaseEngine/Linq/TableQuery.cs (lines 115-128) — BuildExecutionPlan**
```csharp
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
```

**File: MiniDatabaseEngine/Linq/TableQuery.cs (lines 130-147) — ExecuteAccessPath**
```csharp
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
```

### Interactive Elements

- **Hero visual — three-column "query plan" diagram:**
  | LINQ you wrote | Access path chosen | Cost |
  |---|---|---|
  | `.Where(r => (int)r["Id"] == 42)` | **Index point lookup** — `SelectByKey(42)` | O(log n) |
  | `.Where(r => (int)r["Id"] > 100)` | **Index range scan** — `SelectByPrimaryKeyRange(100, null)` | O(log n + k) |
  | `.Where(r => (string)r["Name"] == "Alice")` | **Full table scan** — `SelectAll()` + filter | O(n) |

  Render as pattern cards with a big chip showing the plan type.
- **Code↔English translation #1** — Execute method. English names the three phases: "pick a door (access path), filter what comes through, sort the result."
- **Code↔English translation #2** — ExecuteAccessPath. Emphasize the if-ladder: "exact key? one jump. range? leaf-walk. neither? full scan."
- **Code↔English translation #3** — SelectByKey on Table. Simple — but points at how the LINQ layer eventually lands on a B+ tree `Search`.
- **Interactive layer-toggle demo** — Three tabs: "LINQ source", "Plan", "Physical ops".
  - Tab 1 shows the user's LINQ code.
  - Tab 2 shows the plan object: predicates list, IndexRange, order by.
  - Tab 3 shows which method on Table was actually called.
  This demonstrates the translation from declarative intent → imperative execution.
- **Spot-the-bug challenge** — Show a LINQ query that *looks* like it should use the index but won't:
  ```csharp
  var x = db.Query("Users")
      .Where(r => ((int)r["Id"]).ToString() == "42")
      .ToList();
  ```
  Clicking the `.ToString()` line reveals: "The planner parses `r[\"Id\"] == constant` patterns. Wrapping the column in a function call breaks pattern-matching — it falls back to full scan." This is the same real-world footgun as `WHERE TO_STRING(id) = '42'` in SQL.
- **Callout (accent):** "**Declarative vs imperative.** LINQ describes *what* you want; the planner figures out *how*. This is the single biggest reason databases are productive — you don't hand-code loops."
- **Callout (warning):** "**There are no secondary indexes in this engine.** Only the primary key is indexed. `.Where(r => r[\"Name\"] == \"Alice\")` will always full-scan. Knowing this stops you from blaming AI or random slowdowns."
- **Quiz (4 Qs):**
  1. *Scenario:* `.Where(r => (int)r["Age"] > 25).Where(r => (int)r["Id"] == 7)` — which Where helps the index? (Answer: The `Id == 7` one; the Age filter is applied in-memory afterward.)
  2. *Debugging:* A user complains a query got slower after adding an ORDER BY. Why? (Answer: Sorting is always done in-memory after filtering; adding ORDER BY doesn't hurt the access path but does add a final sort step over all matched rows.)
  3. *Architecture:* If you wanted to add a secondary index on `Email`, which class would you change? (Answer: `Table` — it would hold multiple B+ trees. `TableQuery` would then need a visitor smart enough to pick among them.)
  4. *Tracing:* Given `.Where(r => (int)r["Id"] >= 10 && (int)r["Id"] < 20)`, describe the plan. (Answer: Access path = range scan over `[10, 20)` on the primary key; no in-memory filter needed since both bounds folded into `IndexRange`.)

### Reference Files to Read
- `references/content-philosophy.md`
- `references/gotchas.md`
- `references/interactive-elements.md` → "Code ↔ English Translation Blocks", "Pattern/Feature Cards", "Layer Toggle Demo", "Spot the Bug Challenge", "Callout Boxes", "Multiple-Choice Quizzes", "Glossary Tooltips"

### Tooltips
`query`, `LINQ`, `expression tree`, `visitor pattern`, `predicate`, `access path`, `full scan`, `point lookup`, `range scan`, `declarative`, `imperative`, `pushdown`, `secondary index`, `ORDER BY`, `lambda`, `IQueryable`.

### Connections
- **Previous module:** Module 3 built the B+ tree. This module shows *when* it's consulted.
- **Next module:** Module 5, "Transactions, WAL & Recovery." Writes are more interesting than reads — they need to be durable even across crashes.
- **Tone/style notes:** Teal accent. Use GPS metaphor consistently. Avoid using "cost" numbers like "42ms" — say Big-O instead to stay honest.
