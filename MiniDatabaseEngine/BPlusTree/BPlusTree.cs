using MiniDatabaseEngine.Storage;

namespace MiniDatabaseEngine.BPlusTree;

/// <summary>
/// B+ Tree implementation for indexing and storing data
/// </summary>
public class BPlusTree
{
    private readonly int _order; // Maximum number of children per node
    private readonly DataType _keyType;
    private readonly IComparer<object> _comparer;
    private BPlusTreeNode _root;
    private readonly object _lockObject = new object();
    
    public BPlusTree(int order, DataType keyType)
    {
        if (order < 3)
            throw new ArgumentException("Order must be at least 3", nameof(order));
            
        _order = order;
        _keyType = keyType;
        _comparer = new DataTypeComparer(keyType);
        _root = new BPlusTreeLeafNode();
    }
    
    public BPlusTreeNode Root => _root;
    
    /// <summary>
    /// Insert a key-value pair into the B+ Tree
    /// </summary>
    public void Insert(object key, object? value)
    {
        lock (_lockObject)
        {
            var leaf = FindLeafNode(key);
            InsertIntoLeaf(leaf, key, value);
            
            if (leaf.KeyCount > _order - 1)
            {
                SplitLeafNode(leaf);
            }
        }
    }
    
    /// <summary>
    /// Search for a value by key
    /// </summary>
    public object? Search(object key)
    {
        lock (_lockObject)
        {
            var leaf = FindLeafNode(key);
            int index = FindKeyIndex(leaf.Keys, key);
            
            if (index < leaf.Keys.Count && _comparer.Compare(leaf.Keys[index], key) == 0)
            {
                return ((BPlusTreeLeafNode)leaf).Values[index];
            }
            
            return null;
        }
    }
    
    /// <summary>
    /// Delete a key-value pair from the B+ Tree
    /// </summary>
    public bool Delete(object key)
    {
        lock (_lockObject)
        {
            var leaf = FindLeafNode(key);
            int index = FindKeyIndex(leaf.Keys, key);
            
            if (index < leaf.Keys.Count && _comparer.Compare(leaf.Keys[index], key) == 0)
            {
                leaf.Keys.RemoveAt(index);
                ((BPlusTreeLeafNode)leaf).Values.RemoveAt(index);
                
                // Handle underflow if necessary (simplified - no rebalancing)
                return true;
            }
            
            return false;
        }
    }
    
    /// <summary>
    /// Get all key-value pairs in sorted order
    /// </summary>
    public IEnumerable<KeyValuePair<object, object?>> GetAll()
    {
        lock (_lockObject)
        {
            var leaf = GetFirstLeaf();
            
            while (leaf != null)
            {
                for (int i = 0; i < leaf.Keys.Count; i++)
                {
                    yield return new KeyValuePair<object, object?>(leaf.Keys[i], leaf.Values[i]);
                }
                
                leaf = leaf.Next;
            }
        }
    }
    
    /// <summary>
    /// Get all key-value pairs within a range
    /// </summary>
    public IEnumerable<KeyValuePair<object, object?>> Range(object? minKey, object? maxKey)
    {
        lock (_lockObject)
        {
            var leaf = minKey != null ? FindLeafNode(minKey) : GetFirstLeaf();
            
            while (leaf != null)
            {
                for (int i = 0; i < leaf.Keys.Count; i++)
                {
                    var key = leaf.Keys[i];
                    
                    if (minKey != null && _comparer.Compare(key, minKey) < 0)
                        continue;
                        
                    if (maxKey != null && _comparer.Compare(key, maxKey) > 0)
                        yield break;
                        
                    yield return new KeyValuePair<object, object?>(key, leaf.Values[i]);
                }
                
                leaf = leaf.Next;
            }
        }
    }
    
    private BPlusTreeLeafNode FindLeafNode(object key)
    {
        var current = _root;
        
        while (!current.IsLeaf)
        {
            var internalNode = (BPlusTreeInternalNode)current;
            int childIndex = 0;
            
            // Find the appropriate child to follow
            for (int i = 0; i < current.Keys.Count; i++)
            {
                if (_comparer.Compare(key, current.Keys[i]) >= 0)
                {
                    childIndex = i + 1;
                }
                else
                {
                    break;
                }
            }
            
            current = internalNode.Children[childIndex];
        }
        
        return (BPlusTreeLeafNode)current;
    }
    
    private BPlusTreeLeafNode GetFirstLeaf()
    {
        var current = _root;
        
        while (!current.IsLeaf)
        {
            var internalNode = (BPlusTreeInternalNode)current;
            current = internalNode.Children[0];
        }
        
        return (BPlusTreeLeafNode)current;
    }
    
    private int FindKeyIndex(List<object> keys, object key)
    {
        int left = 0;
        int right = keys.Count;
        
        while (left < right)
        {
            int mid = (left + right) / 2;
            if (_comparer.Compare(keys[mid], key) < 0)
                left = mid + 1;
            else
                right = mid;
        }
        
        return left;
    }
    
    private void InsertIntoLeaf(BPlusTreeLeafNode leaf, object key, object? value)
    {
        int index = FindKeyIndex(leaf.Keys, key);
        
        // Update if key exists
        if (index < leaf.Keys.Count && _comparer.Compare(leaf.Keys[index], key) == 0)
        {
            leaf.Values[index] = value;
            return;
        }
        
        // Insert new key-value pair
        leaf.Keys.Insert(index, key);
        leaf.Values.Insert(index, value);
    }
    
    private void SplitLeafNode(BPlusTreeLeafNode leaf)
    {
        int mid = leaf.Keys.Count / 2;
        
        var newLeaf = new BPlusTreeLeafNode();
        newLeaf.Keys.AddRange(leaf.Keys.GetRange(mid, leaf.Keys.Count - mid));
        newLeaf.Values.AddRange(leaf.Values.GetRange(mid, leaf.Values.Count - mid));
        
        leaf.Keys.RemoveRange(mid, leaf.Keys.Count - mid);
        leaf.Values.RemoveRange(mid, leaf.Values.Count - mid);
        
        // Update linked list
        newLeaf.Next = leaf.Next;
        newLeaf.Previous = leaf;
        if (leaf.Next != null)
            leaf.Next.Previous = newLeaf;
        leaf.Next = newLeaf;
        
        // Promote middle key to parent
        var promotedKey = newLeaf.Keys[0];
        InsertIntoParent(leaf, promotedKey, newLeaf);
    }
    
    private void InsertIntoParent(BPlusTreeNode left, object key, BPlusTreeNode right)
    {
        if (left == _root)
        {
            var newRoot = new BPlusTreeInternalNode();
            newRoot.Keys.Add(key);
            newRoot.Children.Add(left);
            newRoot.Children.Add(right);
            left.Parent = newRoot;
            right.Parent = newRoot;
            _root = newRoot;
            return;
        }
        
        var parent = (BPlusTreeInternalNode)left.Parent!;
        int index = FindKeyIndex(parent.Keys, key);
        
        parent.Keys.Insert(index, key);
        parent.Children.Insert(index + 1, right);
        right.Parent = parent;
        
        if (parent.KeyCount > _order - 1)
        {
            SplitInternalNode(parent);
        }
    }
    
    private void SplitInternalNode(BPlusTreeInternalNode node)
    {
        int mid = node.Keys.Count / 2;
        var promotedKey = node.Keys[mid];
        
        var newNode = new BPlusTreeInternalNode();
        newNode.Keys.AddRange(node.Keys.GetRange(mid + 1, node.Keys.Count - mid - 1));
        newNode.Children.AddRange(node.Children.GetRange(mid + 1, node.Children.Count - mid - 1));
        
        foreach (var child in newNode.Children)
            child.Parent = newNode;
        
        node.Keys.RemoveRange(mid, node.Keys.Count - mid);
        node.Children.RemoveRange(mid + 1, node.Children.Count - mid - 1);
        
        InsertIntoParent(node, promotedKey, newNode);
    }
    
    private class DataTypeComparer : IComparer<object>
    {
        private readonly DataType _dataType;
        
        public DataTypeComparer(DataType dataType)
        {
            _dataType = dataType;
        }
        
        public int Compare(object? x, object? y)
        {
            return DataSerializer.Compare(x, y, _dataType);
        }
    }
}
