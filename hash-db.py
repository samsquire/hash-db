import pygtrie

class Db():
    def __init__(self):
        self.db = {}
        self.sort_index = pygtrie.CharTrie()

    def store(self, partition_key, sort_key, item):
        self.db[partition_key + ":" + sort_key] = item
        self.sort_index[partition_key + ":" + sort_key] = sort_key
        if sort_key not in self.sort_index:
            self.sort_index[sort_key] = pygtrie.CharTrie()
        self.sort_index[sort_key][partition_key] = partition_key + ":" + sort_key

    def query_begins(self, partition_key, query):
        for sort_key, lookup_key in self.sort_index.items(prefix=partition_key + ":" + query):
            yield (lookup_key, self.db[sort_key])

    def query_pk_begins(self, partition_key, query):
        for sort_key, value in self.sort_index.items(prefix=partition_key):
            for partition_key, value in value.items(prefix=query):
                yield (partition_key, self.db[value])

    def query_between(self, partition_key, from_query, to_query):
        for sort_key, lookup_key in self.sort_index.items(prefix=partition_key + ":" + from_query):
            yield (lookup_key, self.db[sort_key])
        for sort_key, lookup_key in self.sort_index.items(prefix=partition_key + ":" + to_query):
            yield (lookup_key, self.db[sort_key])





db = Db()
db.store("user#samsquire", "following#dinar", ["Messages 1"])
db.store("user#samsquire", "following#someonelse", ["Messages 2"])
db.store("user#samsquire", "message#2020-08-01T14:39", ["Messages 1"])
db.store("user#samsquire", "profile", ["profile"])
db.store("user#samsquire", "message#2020-07-01T14:39", ["Messages 2"])
db.store("user#samsquire", "message#2020-06-01T09:30", ["Messages 3"])
db.store("user#samsquire", "message#2020-06-01T14:39", ["Messages 4"])
db.store("user#dinar", "message#2020-06-01T14:39", ["Messages 5"])

print("find followers of user user#samsquire")
print(list(db.query_begins("user#samsquire", "following")))

print("find messages by samsquire between dates")
print(list(db.query_between("user#samsquire", "message#2020-06-01", "message#2020-07-01")))

print("find all users who sent messages")
print(list(db.query_pk_begins("message", "user")))
