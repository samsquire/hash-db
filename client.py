import os
import requests
from argparse import ArgumentParser
from flask import Flask, request, make_response

parser = ArgumentParser()
parser.add_argument("--server")
parser.add_argument("--port")
args = parser.parse_args(os.environ["HASHDB_ARGS"].split(" "))

response = requests.post("http://{}/bootstrap/{}".format(args.server, args.port))
print(response.text)

app = Flask(__name__)

data = {}

@app.route("/get/<lookup_key>", methods=["POST"])
def get(lookup_key):
    return make_response(str(data[lookup_key]))

@app.route("/set/<lookup_key>", methods=["POST"])
def set(lookup_key):
    data[lookup_key] = request.data.decode('utf-8')
    return make_response('', 202)

@app.route("/ping", methods=["POST"])
def ping():
    length = len(data)
    return make_response(str(length), 202)
