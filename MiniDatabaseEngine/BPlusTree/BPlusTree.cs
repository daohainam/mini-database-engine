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
    private readonly object _lockObject = new();
    
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
    /// Delete a key-value pair from the B+ Tree.
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

                if (leaf == _root)
                    return true;

                if (leaf.KeyCount < MinLeafKeys())
                    RebalanceLeafNode(leaf);

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
            var results = new List<KeyValuePair<object, object?>>();
            var leaf = GetFirstLeaf();
            
            while (leaf != null)
            {
                for (int i = 0; i < leaf.Keys.Count; i++)
                {
                    results.Add(new KeyValuePair<object, object?>(leaf.Keys[i], leaf.Values[i]));
                }
                
                leaf = leaf.Next;
            }
            
            return results;
        }
    }
    
    /// <summary>
    /// Get all key-value pairs within a range
    /// </summary>
    public IEnumerable<KeyValuePair<object, object?>> Range(object? minKey, object? maxKey)
    {
        lock (_lockObject)
        {
            var results = new List<KeyValuePair<object, object?>>();
            var leaf = minKey != null ? FindLeafNode(minKey) : GetFirstLeaf();
            
            while (leaf != null)
            {
                for (int i = 0; i < leaf.Keys.Count; i++)
                {
                    var key = leaf.Keys[i];
                    
                    if (minKey != null && _comparer.Compare(key, minKey) < 0)
                        continue;
                        
                    if (maxKey != null && _comparer.Compare(key, maxKey) > 0)
                        return results;
                        
                    results.Add(new KeyValuePair<object, object?>(key, leaf.Values[i]));
                }
                
                leaf = leaf.Next;
            }
            
            return results;
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
        
        // Parent should never be null for non-root nodes
        if (left.Parent == null)
        {
            throw new InvalidOperationException("Parent node is null for non-root node during split operation");
        }
        
        var parent = (BPlusTreeInternalNode)left.Parent;
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

    private int MinLeafKeys() => _order / 2;

    private int MinInternalKeys() => ((_order + 1) / 2) - 1;

    private void RebalanceLeafNode(BPlusTreeLeafNode leaf)
    {
        if (leaf.Parent is not BPlusTreeInternalNode parent)
            return;

        int nodeIndex = parent.Children.IndexOf(leaf);
        if (nodeIndex < 0)
            throw new InvalidOperationException("Leaf node was not found in parent children.");

        var leftSibling = nodeIndex > 0 ? parent.Children[nodeIndex - 1] as BPlusTreeLeafNode : null;
        var rightSibling = nodeIndex < parent.Children.Count - 1 ? parent.Children[nodeIndex + 1] as BPlusTreeLeafNode : null;

        if (leftSibling != null && leftSibling.KeyCount > MinLeafKeys())
        {
            var borrowedKey = leftSibling.Keys[^1];
            var borrowedValue = leftSibling.Values[^1];
            leftSibling.Keys.RemoveAt(leftSibling.Keys.Count - 1);
            leftSibling.Values.RemoveAt(leftSibling.Values.Count - 1);

            leaf.Keys.Insert(0, borrowedKey);
            leaf.Values.Insert(0, borrowedValue);
            parent.Keys[nodeIndex - 1] = leaf.Keys[0];
            return;
        }

        if (rightSibling != null && rightSibling.KeyCount > MinLeafKeys())
        {
            var borrowedKey = rightSibling.Keys[0];
            var borrowedValue = rightSibling.Values[0];
            rightSibling.Keys.RemoveAt(0);
            rightSibling.Values.RemoveAt(0);

            leaf.Keys.Add(borrowedKey);
            leaf.Values.Add(borrowedValue);
            parent.Keys[nodeIndex] = rightSibling.Keys[0];
            return;
        }

        if (leftSibling != null)
        {
            leftSibling.Keys.AddRange(leaf.Keys);
            leftSibling.Values.AddRange(leaf.Values);
            leftSibling.Next = leaf.Next;
            if (leaf.Next != null)
                leaf.Next.Previous = leftSibling;

            parent.Children.RemoveAt(nodeIndex);
            parent.Keys.RemoveAt(nodeIndex - 1);
            RebalanceInternalNode(parent);
            return;
        }

        if (rightSibling != null)
        {
            leaf.Keys.AddRange(rightSibling.Keys);
            leaf.Values.AddRange(rightSibling.Values);
            leaf.Next = rightSibling.Next;
            if (rightSibling.Next != null)
                rightSibling.Next.Previous = leaf;

            parent.Children.RemoveAt(nodeIndex + 1);
            parent.Keys.RemoveAt(nodeIndex);
            RebalanceInternalNode(parent);
        }
    }

    private void RebalanceInternalNode(BPlusTreeInternalNode node)
    {
        if (node == _root)
        {
            if (node.Children.Count == 1)
            {
                _root = node.Children[0];
                _root.Parent = null;
            }
            return;
        }

        if (node.KeyCount >= MinInternalKeys())
            return;

        if (node.Parent is not BPlusTreeInternalNode parent)
            throw new InvalidOperationException("Internal node parent is invalid.");

        int nodeIndex = parent.Children.IndexOf(node);
        if (nodeIndex < 0)
            throw new InvalidOperationException("Internal node was not found in parent children.");

        var leftSibling = nodeIndex > 0 ? parent.Children[nodeIndex - 1] as BPlusTreeInternalNode : null;
        var rightSibling = nodeIndex < parent.Children.Count - 1 ? parent.Children[nodeIndex + 1] as BPlusTreeInternalNode : null;

        if (leftSibling != null && leftSibling.KeyCount > MinInternalKeys())
        {
            var borrowedChild = leftSibling.Children[^1];
            leftSibling.Children.RemoveAt(leftSibling.Children.Count - 1);

            var parentKey = parent.Keys[nodeIndex - 1];
            var borrowedKey = leftSibling.Keys[^1];
            leftSibling.Keys.RemoveAt(leftSibling.Keys.Count - 1);

            node.Keys.Insert(0, parentKey);
            node.Children.Insert(0, borrowedChild);
            borrowedChild.Parent = node;
            parent.Keys[nodeIndex - 1] = borrowedKey;
            return;
        }

        if (rightSibling != null && rightSibling.KeyCount > MinInternalKeys())
        {
            var borrowedChild = rightSibling.Children[0];
            rightSibling.Children.RemoveAt(0);

            var parentKey = parent.Keys[nodeIndex];
            var borrowedKey = rightSibling.Keys[0];
            rightSibling.Keys.RemoveAt(0);

            node.Keys.Add(parentKey);
            node.Children.Add(borrowedChild);
            borrowedChild.Parent = node;
            parent.Keys[nodeIndex] = borrowedKey;
            return;
        }

        if (leftSibling != null)
        {
            var separator = parent.Keys[nodeIndex - 1];
            leftSibling.Keys.Add(separator);
            leftSibling.Keys.AddRange(node.Keys);
            foreach (var child in node.Children)
            {
                leftSibling.Children.Add(child);
                child.Parent = leftSibling;
            }

            parent.Children.RemoveAt(nodeIndex);
            parent.Keys.RemoveAt(nodeIndex - 1);
            RebalanceInternalNode(parent);
            return;
        }

        if (rightSibling != null)
        {
            var separator = parent.Keys[nodeIndex];
            node.Keys.Add(separator);
            node.Keys.AddRange(rightSibling.Keys);
            foreach (var child in rightSibling.Children)
            {
                node.Children.Add(child);
                child.Parent = node;
            }

            parent.Children.RemoveAt(nodeIndex + 1);
            parent.Keys.RemoveAt(nodeIndex);
            RebalanceInternalNode(parent);
        }
    }

    public void ValidateInvariants()
    {
        lock (_lockObject)
        {
            if (_root == null)
                throw new InvalidOperationException("Root node cannot be null.");

            int? leafDepth = null;
            ValidateNode(_root, isRoot: true, depth: 0, ref leafDepth, lowerBound: null, upperBound: null);
            ValidateLeafChain();
        }
    }

    private void ValidateNode(
        BPlusTreeNode node,
        bool isRoot,
        int depth,
        ref int? leafDepth,
        object? lowerBound,
        object? upperBound)
    {
        if (node.KeyCount > _order - 1)
            throw new InvalidOperationException("Node exceeds maximum key count.");

        for (int i = 1; i < node.Keys.Count; i++)
        {
            if (_comparer.Compare(node.Keys[i - 1], node.Keys[i]) >= 0)
                throw new InvalidOperationException("Node keys must be strictly increasing.");
        }

        if (!isRoot)
        {
            int minKeys = node.IsLeaf ? MinLeafKeys() : MinInternalKeys();
            if (node.KeyCount < minKeys)
                throw new InvalidOperationException("Node is under minimum key count.");
        }

        if (node.IsLeaf)
        {
            var leaf = (BPlusTreeLeafNode)node;
            if (leaf.Values.Count != leaf.Keys.Count)
                throw new InvalidOperationException("Leaf values count must match key count.");

            ValidateBounds(leaf.Keys, lowerBound, upperBound);

            if (leafDepth == null)
                leafDepth = depth;
            else if (leafDepth.Value != depth)
                throw new InvalidOperationException("All leaves must be at the same depth.");

            return;
        }

        var internalNode = (BPlusTreeInternalNode)node;
        if (internalNode.Children.Count != internalNode.Keys.Count + 1)
            throw new InvalidOperationException("Internal node children count must equal keys + 1.");

        if (isRoot && internalNode.Children.Count < 2)
            throw new InvalidOperationException("Root internal node must have at least two children.");

        for (int i = 0; i < internalNode.Children.Count; i++)
        {
            if (!ReferenceEquals(internalNode.Children[i].Parent, internalNode))
                throw new InvalidOperationException("Child parent pointer is invalid.");

            object? childLower = i == 0 ? lowerBound : internalNode.Keys[i - 1];
            object? childUpper = i == internalNode.Children.Count - 1 ? upperBound : internalNode.Keys[i];
            ValidateNode(internalNode.Children[i], false, depth + 1, ref leafDepth, childLower, childUpper);

            if (i > 0)
            {
                var expectedSeparator = GetLeftmostKey(internalNode.Children[i]);
                if (_comparer.Compare(internalNode.Keys[i - 1], expectedSeparator) != 0)
                    throw new InvalidOperationException("Internal separator key is inconsistent with right subtree.");
            }
        }
    }

    private void ValidateBounds(IReadOnlyList<object> keys, object? lowerBound, object? upperBound)
    {
        foreach (var key in keys)
        {
            if (lowerBound != null && _comparer.Compare(key, lowerBound) < 0)
                throw new InvalidOperationException("Key is less than lower bound.");

            if (upperBound != null && _comparer.Compare(key, upperBound) >= 0)
                throw new InvalidOperationException("Key is greater than or equal to upper bound.");
        }
    }

    private object GetLeftmostKey(BPlusTreeNode node)
    {
        var current = node;
        while (!current.IsLeaf)
        {
            current = ((BPlusTreeInternalNode)current).Children[0];
        }

        if (current.Keys.Count == 0)
            throw new InvalidOperationException("Encountered empty leaf while validating separators.");

        return current.Keys[0];
    }

    private void ValidateLeafChain()
    {
        var leaf = GetFirstLeaf();
        BPlusTreeLeafNode? previous = null;
        object? previousKey = null;

        while (leaf != null)
        {
            if (!ReferenceEquals(leaf.Previous, previous))
                throw new InvalidOperationException("Leaf previous pointer is inconsistent.");

            foreach (var key in leaf.Keys)
            {
                if (previousKey != null && _comparer.Compare(previousKey, key) >= 0)
                    throw new InvalidOperationException("Leaf chain keys must be strictly increasing.");
                previousKey = key;
            }

            previous = leaf;
            leaf = leaf.Next;
        }
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
