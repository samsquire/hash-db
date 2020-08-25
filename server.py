import time
import pygtrie
from flask import Flask, request, make_response, Response
from consistent_hashing import ConsistentHash
import requests
from threading import Thread
app = Flask(__name__)

hashes = {
    "hashes": None
}
servers = []
sort_index = pygtrie.CharTrie()

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

    sort_index[partition_key + ":" + sort_key] = sort_key
    if sort_key not in sort_index:
        sort_index[sort_key] = pygtrie.CharTrie()
    sort_index[sort_key][partition_key] = partition_key + ":" + sort_key

    return make_response(str(response.status_code), response.status_code)

@app.route("/query_begins/<partition_key>/<query>/<sort_mode>", methods=["POST"])
def query_begins(partition_key, query, sort_mode):
    def items():
        for lookup_key, sort_key in sort_index.iteritems(prefix=partition_key + ":" + query):
            machine_index = hashes["hashes"].get_machine(partition_key + ":" + sort_key)
            server = servers[machine_index]
            print(len(servers))
            response = requests.post("http://{}/get/{}".format(server, lookup_key))
            yield "{}, {}, \"{}\"\n".format(sort_key, lookup_key, response.text)
    return Response(sorted(items(), key=lambda x: x[0], reverse=sort_mode == "desc"), mimetype="text/csv")
