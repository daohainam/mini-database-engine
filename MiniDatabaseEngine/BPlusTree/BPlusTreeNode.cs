namespace MiniDatabaseEngine.BPlusTree;

/// <summary>
/// Represents an entry in a B+ Tree node (key-value pair)
/// </summary>
public class BPlusTreeEntry
{
    public object Key { get; set; }
    public object? Value { get; set; }
    
    public BPlusTreeEntry(object key, object? value = null)
    {
        Key = key;
        Value = value;
    }
}

/// <summary>
/// Base class for B+ Tree nodes
/// </summary>
public abstract class BPlusTreeNode
{
    public bool IsLeaf { get; protected set; }
    public BPlusTreeNode? Parent { get; set; }
    public List<object> Keys { get; protected set; }
    
    protected BPlusTreeNode(bool isLeaf)
    {
        IsLeaf = isLeaf;
        Keys = new List<object>();
    }
    
    public abstract int KeyCount { get; }
}

/// <summary>
/// Internal (non-leaf) node in a B+ Tree
/// </summary>
public class BPlusTreeInternalNode : BPlusTreeNode
{
    public List<BPlusTreeNode> Children { get; private set; }
    
    public BPlusTreeInternalNode() : base(false)
    {
        Children = new List<BPlusTreeNode>();
    }
    
    public override int KeyCount => Keys.Count;
}

/// <summary>
/// Leaf node in a B+ Tree (stores actual data)
/// </summary>
public class BPlusTreeLeafNode : BPlusTreeNode
{
    public List<object?> Values { get; private set; }
    public BPlusTreeLeafNode? Next { get; set; }
    public BPlusTreeLeafNode? Previous { get; set; }
    
    public BPlusTreeLeafNode() : base(true)
    {
        Values = new List<object?>();
    }
    
    public override int KeyCount => Keys.Count;
}
