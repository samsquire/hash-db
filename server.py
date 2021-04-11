import time
import json
import pygtrie
from flask import Flask, request, make_response, Response
from consistent_hashing import ConsistentHash
import requests
from threading import Thread
app = Flask(__name__)
from datastructures import Tree, PartitionTree
from itertools import chain
from functools import reduce
from concurrent.futures import ThreadPoolExecutor

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
    old_hashes = hashes["hashes"]
    hashes["hashes"] = ConsistentHash(num_machines=len(servers), num_replicas=3)
    new_index = len(servers)  - 1
    bootstrapped_keys = {}
    print("Uploading missing data")
    for lookup_key in indexed.keys():
        machine_index = hashes["hashes"].get_machine(lookup_key) 

        if machine_index == new_index:
            print("Found key redistributed to this server") 
            # get the key from elsewhere in the cluster
            old_machine_index = old_hashes.get_machine(lookup_key)
            server = servers[old_machine_index]
            response = requests.post("http://{}/get/{}".format(server, lookup_key))
            old_value = response.text
            requests.post("http://{}/clear/{}".format(server, lookup_key))
            bootstrapped_keys[lookup_key] = old_value
            # upload it back into the cluster onto this machine
        
         
    return make_response(json.dumps(bootstrapped_keys), 202)

@app.route("/set/<partition_key>/<sort_key>", methods=["POST"])
def set(partition_key, sort_key):
    lookup_key = partition_key + ":" + sort_key
    machine_index = hashes["hashes"].get_machine(lookup_key)
    response = requests.post("http://{}/set/{}/{}".format(servers[machine_index], partition_key, sort_key), data=request.data)

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

def fromserver(items):
    for item in items:
        yield "{}, {}, \"{}\"\n".format(item["sort_key"], item["lookup_key"], item["value"])

@app.route("/query_begins/<partition_key>/<query>/<sort_mode>", methods=["GET"])
def query_begins(partition_key, query, sort_mode):


    def items():
        def getresults(server):
            response = requests.post("http://{}/query_begins/{}/{}".format(server, partition_key, query), data=json.dumps({"servers": servers, "hashes": hashes["hashes"].to_dict()}))
            yield json.loads(response.text)          
        with ThreadPoolExecutor(max_workers=len(servers)) as executor:
            future = executor.map(getresults, servers)
            yield from future

        # send query to all servers
    return Response(fromserver(sorted(reduce(lambda previous, current: previous + next(current), items(), []), key=lambda x: x["sort_key"], reverse=sort_mode == "desc")), mimetype="text/plain")

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
        key=lambda x: x[0], reverse=sort_mode == "desc")), mimetype="text/plain")


@app.route("/query_between/<partition_key>/<from_query>/<to_query>/<sort_mode>", methods=["GET"])
def query_between(partition_key, from_query, to_query, sort_mode):
    def items(partition_key, from_query, to_query):
        for partition_key, sort_key, lookup_key in between_index[partition_key].walk(from_query, to_query):
            machine_index = hashes["hashes"].get_machine(lookup_key)
            server = servers[machine_index]

            response = requests.post("http://{}/get/{}".format(server, lookup_key))
            yield (sort_key, lookup_key, response.text)

    return Response(asstring(sorted(items(partition_key, from_query, to_query),
        key=lambda x: x[0], reverse=sort_mode == "desc")), mimetype="text/plain")

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
        key=lambda x: x[0], reverse=sort_mode == "desc")), mimetype="text/plain")
