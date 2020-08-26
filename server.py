import time
import pygtrie
from flask import Flask, request, make_response, Response
from consistent_hashing import ConsistentHash
import requests
from threading import Thread
app = Flask(__name__)

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
        return self

    def walk(self, less_than, stop):
        if self.left:
            yield from self.left.walk(less_than, stop)
        if less_than <= self.value and self.value <= stop:
            yield self.value, self.partition_tree
        if self.right:
            yield from self.right.walk(less_than, stop)


hashes = {
    "hashes": None
}
indexed = {}
servers = []
sort_index = pygtrie.CharTrie()
between_index = {}
both_between_index = PartitionTree("", None)
partition_trees = {}

class Worker(Thread):
    def __init__(self):
        super(Worker, self).__init__()
    def run(self):
        while True:
            time.sleep(5)
            removed_servers = []
            for server in servers:
                try:
                    response = requests.post("http://{}/ping".format(server))
                    print("Server {} is up serving {} keys".format(server, response.text))
                except:
                    print("Server gone away")
                    removed_servers.append(server)
            for server in removed_servers:
                servers.remove(server)

Worker().start()

@app.route("/bootstrap/<port>", methods=["POST"])
def bootstrap(port):
    servers.append(request.remote_addr + ":" + port)
    hashes["hashes"] = ConsistentHash(num_machines=len(servers), num_replicas=3)
    return make_response('', 202)

@app.route("/set/<partition_key>/<sort_key>", methods=["POST"])
def set(partition_key, sort_key):
    lookup_key = partition_key + ":" + sort_key
    machine_index = hashes["hashes"].get_machine(lookup_key)
    response = requests.post("http://{}/set/{}".format(servers[machine_index], lookup_key), data=request.data)

    if lookup_key in indexed:
        return make_response(str(response.status_code), response.status_code)

    indexed[lookup_key] = True
    sort_index[partition_key + ":" + sort_key] = sort_key
    if sort_key not in sort_index:
        sort_index[sort_key] = pygtrie.CharTrie()
    sort_index[sort_key][partition_key] = partition_key + ":" + sort_key
    if partition_key not in between_index:
        between_index[partition_key] = Tree("", None, None)
    if partition_key not in partition_trees:
        partition_tree = both_between_index.insert(partition_key, Tree("", None, None))
        partition_trees[partition_key] = partition_tree
    between_index[partition_key].insert(sort_key, partition_key, partition_key + ":" + sort_key)
    partition_trees[partition_key].partition_tree.insert(sort_key, partition_key, partition_key + ":" + sort_key)


    return make_response(str(response.status_code), response.status_code)

def asstring(items):
    for item in items:
        yield "{}, {}, \"{}\"\n".format(item[0], item[1], item[2])

@app.route("/query_begins/<partition_key>/<query>/<sort_mode>", methods=["POST"])
def query_begins(partition_key, query, sort_mode):

    def items():
        for lookup_key, sort_key in sort_index.iteritems(prefix=partition_key + ":" + query):
            machine_index = hashes["hashes"].get_machine(lookup_key)
            server = servers[machine_index]

            response = requests.post("http://{}/get/{}".format(server, lookup_key))
            yield (sort_key, lookup_key, response.text)
    return Response(asstring(sorted(items(), key=lambda x: x[0], reverse=sort_mode == "desc")), mimetype="text/csv")

@app.route("/query_pk_sk_begins/<partition_key_query>/<query>/<sort_mode>", methods=["GET"])
def query_pk_begins(partition_key_query, query, sort_mode):
    def items(partition_key_query, query):
        for sort_key, value in sort_index.iteritems(prefix=query):
            for partition_key, lookup_key in value.iteritems(prefix=partition_key_query):
                machine_index = hashes["hashes"].get_machine(lookup_key)
                server = servers[machine_index]

                response = requests.post("http://{}/get/{}".format(server, lookup_key))
                yield (sort_key, lookup_key, response.text)

    return Response(asstring(sorted(items(partition_key_query, query),
        key=lambda x: x[0], reverse=sort_mode == "desc")), mimetype="text/csv")


@app.route("/query_between/<partition_key>/<from_query>/<to_query>/<sort_mode>", methods=["GET"])
def query_between(partition_key, from_query, to_query, sort_mode):
    def items(partition_key, from_query, to_query):
        for partition_key, sort_key, lookup_key in between_index[partition_key].walk(from_query, to_query):
            machine_index = hashes["hashes"].get_machine(lookup_key)
            server = servers[machine_index]

            response = requests.post("http://{}/get/{}".format(server, lookup_key))
            yield (sort_key, lookup_key, response.text)

    return Response(asstring(sorted(items(partition_key, from_query, to_query),
        key=lambda x: x[0], reverse=sort_mode == "desc")), mimetype="text/csv")

@app.route("/both_between/<from_partition_key>/<to_partition_key>/<from_query>/<to_query>/<sort_mode>", methods=["GET"])
def both_between(from_partition_key, to_partition_key, from_query, to_query, sort_mode):
    def items(from_partition_key, to_partition_key, from_query, to_query):
        for partition_key, partition_tree in both_between_index.walk(from_partition_key, to_partition_key):
            for partition_key, sort_key, lookup_key in partition_tree.walk(from_query, to_query):
                machine_index = hashes["hashes"].get_machine(lookup_key)
                server = servers[machine_index]

                response = requests.post("http://{}/get/{}".format(server, lookup_key))
                yield (sort_key, lookup_key, response.text)

    return Response(asstring(sorted(items(from_partition_key, to_partition_key, from_query, to_query),
        key=lambda x: x[0], reverse=sort_mode == "desc")), mimetype="text/csv")
