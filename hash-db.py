import pygtrie

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

class Db():
    def __init__(self):
        self.db = {}
        self.sort_index = pygtrie.CharTrie()
        self.between_index = {}

    def store(self, partition_key, sort_key, item):
        lookup_key = partition_key + ":" + sort_key
        self.db[lookup_key] = item
        self.sort_index[lookup_key] = sort_key
        if sort_key not in self.sort_index:
            self.sort_index[sort_key] = pygtrie.CharTrie()
        self.sort_index[sort_key][partition_key] = lookup_key
        if partition_key not in self.between_index:
            self.between_index[partition_key] = Tree("", None, None)
        self.between_index[partition_key].insert(sort_key, partition_key, lookup_key)

    def query_begins(self, partition_key, query, sortmode):
        def items():
            for sort_key, lookup_key in self.sort_index.iteritems(prefix=partition_key + ":" + query):
                yield (sort_key, lookup_key, self.db[sort_key])
        return sorted(items(), key=lambda x: x[0], reverse=sortmode == "desc")


    def query_pk_begins(self, partition_key, query, sortmode):
        def items(partition_key, query):
            for sort_key, value in self.sort_index.iteritems(prefix=partition_key):
                for partition_key, value in value.iteritems(prefix=query):
                    yield (sort_key, partition_key, self.db[value])
        return sorted(items(partition_key, query), key=lambda x: x[0], reverse=sortmode == "desc")

    def query_between(self, partition_key, from_query, to_query, sortmode):
        def items(partition_key, from_query, to_query):
            for partition_key, sort_key, lookup_key in self.between_index[partition_key].walk(from_query, to_query):
                yield (sort_key, partition_key, self.db[lookup_key])
        return sorted(items(partition_key, from_query, to_query), key=lambda x: x[0], reverse=sortmode == "desc")

    def query_before_than(self, partition_key, prefix, target_sort_key, sortmode):
        def less_than():
            for lookup_key, sort_key in self.sort_index.items(prefix=partition_key + ":" + prefix):
                if sort_key < target_sort_key:
                    yield (sort_key, lookup_key, self.db[lookup_key])
        return sorted(less_than(), key=lambda x: x[0], reverse=sortmode == "desc")

    def query_greater_than(self, partition_key, prefix, target_sort_key, sortmode):
        def less_than():
            for lookup_key, sort_key in self.sort_index.items(prefix=partition_key + ":" + prefix):
                if sort_key > target_sort_key:
                    yield (sort_key, lookup_key, self.db[lookup_key])
        return sorted(less_than(), key=lambda x: x[0], reverse=sortmode == "desc")



db = Db()
db.store("user#samsquire", "following#dinar", ["Messages 1"])
db.store("user#samsquire", "following#someonelse", ["Messages 2"])
db.store("user#samsquire", "message#2020-05-01T14:39", ["Messages 1"])
db.store("user#samsquire", "profile", ["profile"])
db.store("user#samsquire", "message#2020-06-01T14:39", ["Messages 2"])
db.store("user#samsquire", "message#2020-07-01T09:30", ["Messages 3"])
db.store("user#samsquire", "message#2020-08-01T14:39", ["Messages 4"])
db.store("user#dinar", "message#2020-09-01T14:39", ["Messages 5"])

print("find followers of user user#samsquire")
print(list(db.query_begins("user#samsquire", "following", sortmode="asc")))

print("find messages by samsquire between dates")
print(list(db.query_between("user#samsquire", "message#2020-06-01", "message#2020-07-01~~~~~~~~~", sortmode="asc")))

print("find all users who sent messages")
print(list(db.query_pk_begins("message", "user", sortmode="desc")))

print("before than")
print(list(db.query_before_than("user#samsquire", "message", "message#2020-07", "asc")))

print("greater than")
print(list(db.query_greater_than("user#samsquire", "message", "message#2020-07", "asc")))
