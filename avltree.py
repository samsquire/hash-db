
# https://github.com/aksh0001/algorithms-journal/blob/master/data_structures/trees/AVLTree.py
class TreeNode:
    def __init__(self, key, value, balance=0):
        self.key = key
        self.value = value
        self.height = 1
        self.balance = balance
        self.left = None
        self.right = None

    def is_leaf(self):
        return self.height == 1

    @property
    def left_height(self) -> int:
        if self.left:
            return self.left.height
        else:
            return 0

    @property
    def right_height(self) -> int:
        if self.right:
            return self.right.height
        else:
            return 0

    def before(self) -> 'TreeNode':
        node = self.left
        if node:
            while node.right:
                if node.right.node == None:
                    return node
                else:
                    node = node.right.node
        return node

    def after(self) -> 'TreeNode':
        node = self.right
        if node:
            while node.left:
                if node.left.node == None:
                    return node
                else:
                    node = node.left.node
        return node

    def update(self):
        left, right = self.left_height, self.right_height
        self.height = 1 + max(left, right)
        self.balance = left - right


class AVLTree():
    def __init__(self, key, value):
        self.root = TreeNode(key, value)
        self.size = 1

    @property
    def height(self):
        if self.root:
            return self.root.height
        else:
            return 0

    def insert(self, key, value) -> TreeNode:
        new = TreeNode(key, value)
        self.root = self._insert(self.root, new)
        self.size += 1
        return new

    def _insert(self, node: TreeNode, new: TreeNode) -> TreeNode:
        if not node:
            return new

        if new.key < node.key:
            node.left = self._insert(node.left, new)
        elif new.key > node.key:
            node.right = self._insert(node.right, new)
        else:
            return node

        node.update()
        return self.rebalance(node)

    def rebalance(self, node: TreeNode) -> TreeNode:
        if node.balance == 2:
            if node.left.balance < 0:
                node.left = self.rotate_left(node.left)
                return self.rotate_right(node)
            else:
                return self.rotate_right(node)
        elif node.balance == -2:
            if node.right.balance > 0:
                node.right = self.rotate_right(node.right)
                return self.rotate_left(node)
            else:
                return self.rotate_left(node)
        else:
            return node

    def rotate_left(self, node: TreeNode) -> TreeNode:
        pivot = node.right
        tmp = pivot.left

        pivot.left = node
        node.right = tmp

        node.update()
        pivot.update()
        return pivot

    def rotate_right(self, node: TreeNode) -> TreeNode:
        pivot = node.left
        tmp = pivot.right

        pivot.right = node
        node.left = tmp

        node.update()
        pivot.update()
        return pivot
