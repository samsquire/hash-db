from datastructures import Tree

def make_tree_node(value, partition_key='part_key', lookup_key='lookup_key'):
    return Tree(value, partition_key, lookup_key)

def test_tree_init():
    root = make_tree_node(0)

    assert root.value == 0
    assert root.left == None
    assert root.right == None
    assert root.partition_key == 'part_key'
    assert root.lookup_key == 'lookup_key'

def test_tree_insert_right():
    root = make_tree_node(0)
    root.insert(1, 'part_key', 'lookup_key')

    assert root.left == None
    assert root.right != None
    right = root.right
    assert right.value == 1

def test_tree_insert_left():
    root = make_tree_node(0)
    root.insert(-1, 'part_key', 'lookup_key')

    assert root.left != None
    assert root.right == None
    left = root.left
    assert left.value == -1

def test_tree_insert_left_left():
    root = make_tree_node(0)
    root.insert(-1, 'part_key', 'lookup_key')
    root.insert(-2, 'part_key', 'lookup_key')

    assert root.left != None
    assert root.right == None
    left = root.left
    assert left.value == -1
    left = left.left
    assert left.value == -2

def test_tree_insert_delete():
    root = make_tree_node(0)
    root.insert(1, 'part_key', 'lookup_key')
    root.delete(1)

    assert root.left == None
    assert root.right == None

def test_tree_insert_insert_delete():
    root = make_tree_node(0)
    root.insert(1, 'part_key', 'lookup_key')
    root.insert(1, 'part_key', 'lookup_key')
    root.delete(1)

    assert root.left == None
    assert root.right == None

def test_tree_simple_walk():
    root = make_tree_node(0)
    root.insert(-1, 'part_key', 'lookup_key')
    root.insert(1, 'part_key', 'lookup_key')

    actual = [value for (_, value, _) in root.walk(-1, 1)]
    assert actual == [-1, 0, 1]

    root.insert(2, 'part_key', 'lookup_key')

    actual = [value for (_, value, _) in root.walk(-1, 2)]
    assert actual == [-1, 0, 1, 2]

    root.insert(1, 'part_key', 'lookup_key')

    actual = [value for (_, value, _) in root.walk(-1, 1)]
    assert actual == [-1, 0, 1, 1]
