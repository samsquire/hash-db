
class Tree():
    def __init__(self, value, partition_key, lookup_key):
        self.value = value
        self.partition_key = partition_key
        self.lookup_key = lookup_key
        self.left = None
        self.right = None

    def insert(self, value, partition_key, lookup_key):
        if self.left == None and value <= self.value:
            self.left = Tree(value, partition_key, lookup_key)
        elif self.right == None and value > self.value:
            self.right = Tree(value, partition_key, lookup_key)
        elif value > self.value:
            self.right.insert(value, partition_key, lookup_key)
        elif value < self.value:
            self.left.insert(value, partition_key, lookup_key)
        elif self.value == "":
            self.value = value
            self.partition_key = partition_key
            self.lookup_key = lookup_key
        return self

    def walk(self, less_than, stop):
        if self.left:
            yield from self.left.walk(less_than, stop)
        if less_than <= self.value and self.value <= stop:
            yield self.partition_key, self.value, self.lookup_key
        if self.right:
            yield from self.right.walk(less_than, stop)

class PartitionTree():
    def __init__(self, value, partition_tree):
        self.value = value
        self.partition_tree = partition_tree
        self.left = None
        self.right = None

    def insert(self, value, partition_tree):
        if self.left == None and value <= self.value:
            self.left = PartitionTree(value, partition_tree)
            return self.left
        elif self.right == None and value > self.value:
            self.right = PartitionTree(value, partition_tree)
            return self.right
        elif value > self.value:
            return self.right.insert(value, partition_tree)
        elif value < self.value:
            return self.left.insert(value, partition_tree)
        elif self.value == "":
            self.value = value
            self.partition_tree = partition_tree
        return self

    def walk(self, less_than, stop):

        if self.left:
            yield from self.left.walk(less_than, stop)
        if less_than <= self.value and self.value <= stop:
            yield self.value, self.partition_tree
        if self.right:
            yield from self.right.walk(less_than, stop)
