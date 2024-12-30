package btree

const (
    degree = 4  // Minimum degree of B-tree
)

type Item struct {
    Key   interface{}
    Value interface{}
}

func (t *BTree) insertNonFull(node *Node, key, value interface{}) {
    i := len(node.Keys) - 1
    if node.Leaf {
        node.Keys = append(node.Keys, Item{})
        for i >= 0 && key.(int) < node.Keys[i].Key.(int) {
            node.Keys[i+1] = node.Keys[i]
            i--
        }
        node.Keys[i+1] = Item{Key: key, Value: value}
    } else {
        for i >= 0 && key.(int) < node.Keys[i].Key.(int) {
            i--
        }
        i++
        if len(node.Children[i].Keys) == (2*degree - 1) {
            t.splitChild(node, i)
            if key.(int) > node.Keys[i].Key.(int) {
                i++
            }
        }
        t.insertNonFull(node.Children[i], key, value)
    }
}

type Node struct {
    Leaf     bool
    Keys     []Item
    Children []*Node
}

type BTree struct {
    Root *Node
}

func NewBTree() *BTree {
    return &BTree{
        Root: &Node{
            Leaf: true,
        },
    }
}

func (t *BTree) Insert(key, value interface{}) {
    root := t.Root
    if len(root.Keys) == (2*degree - 1) {
        newRoot := &Node{
            Leaf:     false,
            Children: []*Node{root},
        }
        t.Root = newRoot
        t.splitChild(newRoot, 0)
        t.insertNonFull(newRoot, key, value)
    } else {
        t.insertNonFull(root, key, value)
    }
}

func (t *BTree) Search(key interface{}) interface{} {
    return t.searchNode(t.Root, key)
}

func (t *BTree) searchNode(node *Node, key interface{}) interface{} {
    i := 0
    for i < len(node.Keys) && key.(int) > node.Keys[i].Key.(int) {
        i++
    }
    if i < len(node.Keys) && key.(int) == node.Keys[i].Key.(int) {
        return node.Keys[i].Value
    }
    if node.Leaf {
        return nil
    }
    return t.searchNode(node.Children[i], key)
}

func (t *BTree) splitChild(parent *Node, i int) {
    degree := degree
    child := parent.Children[i]
    newChild := &Node{
        Leaf: child.Leaf,
        Keys: make([]Item, degree-1),
    }
    parent.Children = append(parent.Children[:i+1], append([]*Node{newChild}, parent.Children[i+1:]...)...)
    parent.Keys = append(parent.Keys[:i], append([]Item{child.Keys[degree-1]}, parent.Keys[i:]...)...)
    newChild.Keys = append(newChild.Keys, child.Keys[degree:]...)
    child.Keys = child.Keys[:degree-1]
    if !child.Leaf {
        newChild.Children = append(newChild.Children, child.Children[degree:]...)
        child.Children = child.Children[:degree]
    }
}