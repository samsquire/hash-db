from avltree import AVLTree, TreeNode

def make_avl_tree(key, value):
    return AVLTree(key, value)

def test_tree_init():
    tree = make_avl_tree('Alice', 0)
    root = tree.root

    assert root.key == 'Alice'
    assert root.value == 0
    assert root.left == None
    assert root.right == None

def test_tree_insert_right():
    tree = make_avl_tree('Alice', 0)
    tree.insert('Bob', 1)
    root = tree.root

    assert root.key == 'Alice'
    assert root.value == 0
    assert root.left == None
    assert root.right != None

    right = root.right
    assert right.key == 'Bob'
    assert right.value == 1

def test_tree_insert_right_right():
    tree = make_avl_tree('Alice', 0)
    tree.insert('Bob', 1)
    tree.insert('Charlie', 2)

    root = tree.root
    assert root.height == 2

    assert root.left != None
    left = root.left

    assert left.key == 'Alice'
    assert left.value == 0

    assert root.key == 'Bob'
    assert root.value == 1

    assert root.right != None
    right = root.right

    assert right.key == 'Charlie'
    assert right.value == 2
