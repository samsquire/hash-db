import requests

from argparse import ArgumentParser
parser = ArgumentParser()
parser.add_argument("--server")
args = parser.parse_args()

response = requests.post("http://{}/set/people-100/messages-100".format(args.server), data="People 100")
print(response.text)

response = requests.post("http://{}/set/people-100/messages-101".format(args.server), data="People 101")
print(response.text)

response = requests.post("http://{}/set/people-100/messages-3500".format(args.server), data="People 101")
print(response.text)

response = requests.post("http://{}/set/machines-10/messages-3500".format(args.server), data="Machine 101")
print(response.text)

response = requests.post("http://{}/query_begins/people-100/messages/asc".format(args.server))
print(response.text)
