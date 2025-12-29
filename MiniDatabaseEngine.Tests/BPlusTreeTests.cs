using Xunit;

namespace MiniDatabaseEngine.Tests;

public class BPlusTreeTests
{
    [Fact]
    public void Insert_And_Search_Integer_Keys()
    {
        var tree = new BPlusTree.BPlusTree(4, DataType.Int);
        
        tree.Insert(10, "Value10");
        tree.Insert(20, "Value20");
        tree.Insert(5, "Value5");
        tree.Insert(15, "Value15");
        
        Assert.Equal("Value10", tree.Search(10));
        Assert.Equal("Value20", tree.Search(20));
        Assert.Equal("Value5", tree.Search(5));
        Assert.Equal("Value15", tree.Search(15));
        Assert.Null(tree.Search(99));
    }
    
    [Fact]
    public void Insert_Many_Values_Triggers_Split()
    {
        var tree = new BPlusTree.BPlusTree(4, DataType.Int);
        
        for (int i = 1; i <= 20; i++)
        {
            tree.Insert(i, $"Value{i}");
        }
        
        for (int i = 1; i <= 20; i++)
        {
            Assert.Equal($"Value{i}", tree.Search(i));
        }
    }
    
    [Fact]
    public void Delete_Key_Removes_Value()
    {
        var tree = new BPlusTree.BPlusTree(4, DataType.Int);
        
        tree.Insert(10, "Value10");
        tree.Insert(20, "Value20");
        tree.Insert(5, "Value5");
        
        Assert.True(tree.Delete(10));
        Assert.Null(tree.Search(10));
        Assert.Equal("Value20", tree.Search(20));
        Assert.Equal("Value5", tree.Search(5));
    }
    
    [Fact]
    public void GetAll_Returns_Keys_In_Order()
    {
        var tree = new BPlusTree.BPlusTree(4, DataType.Int);
        
        tree.Insert(20, "Value20");
        tree.Insert(10, "Value10");
        tree.Insert(30, "Value30");
        tree.Insert(5, "Value5");
        
        var all = tree.GetAll().ToList();
        
        Assert.Equal(4, all.Count);
        Assert.Equal(5, all[0].Key);
        Assert.Equal(10, all[1].Key);
        Assert.Equal(20, all[2].Key);
        Assert.Equal(30, all[3].Key);
    }
    
    [Fact]
    public void Range_Query_Returns_Matching_Keys()
    {
        var tree = new BPlusTree.BPlusTree(4, DataType.Int);
        
        for (int i = 1; i <= 20; i++)
        {
            tree.Insert(i, $"Value{i}");
        }
        
        var range = tree.Range(5, 10).ToList();
        
        Assert.Equal(6, range.Count); // 5, 6, 7, 8, 9, 10
        Assert.Equal(5, range[0].Key);
        Assert.Equal(10, range[5].Key);
    }
}
