import requests

from argparse import ArgumentParser
parser = ArgumentParser()
parser.add_argument("--server")
args = parser.parse_args()

response = requests.post("http://{}/set/people-100/messages-100".format(args.server), data="Message 100")
print(response.text)

response = requests.post("http://{}/set/people-100/messages-101".format(args.server), data="Message 101")
print(response.text)

response = requests.post("http://{}/set/people-100/messages-102".format(args.server), data="Message 102")
print(response.text)

response = requests.post("http://{}/set/people-100/messages-103".format(args.server), data="Message 103")
print(response.text)

response = requests.post("http://{}/set/people-100/messages-104".format(args.server), data="Message 104")
print(response.text)

response = requests.post("http://{}/set/people-100/messages-105".format(args.server), data="Message 105")
print(response.text)

response = requests.post("http://{}/set/people-100/messages-3500".format(args.server), data="Message 3500")
print(response.text)

response = requests.post("http://{}/set/people-200/messages-500".format(args.server), data="Message 500")
print(response.text)

response = requests.post("http://{}/set/machines-10/messages-3500".format(args.server), data="Machine 101")
print(response.text)

response = requests.post("http://{}/set/people-100-2020-05-01/friends-2019-05-01".format(args.server), data="1, 2")
print(response.text)

response = requests.post("http://{}/set/people-100-2020-05-01/friends-2020-06-01".format(args.server), data="1, 2, 3")
print(response.text)

print("Query begins asc")
url = "http://{}/query_begins/people-100/messages/asc".format(args.server)
response = requests.get(url)
print(url)
print(response.text)

print("Query begins desc")
url = "http://{}/query_begins/people-100/messages/desc".format(args.server)
print(url)
response = requests.get(url)
print(response.text)

print("PK and SK begins with")
url = "http://{}/query_pk_sk_begins/people/messages/desc".format(args.server)
print(url)
response = requests.get(url)
print(response.text)

print("messages between 101 and 105")
url = "http://{}/query_between/people-100/messages-101/messages-105/desc".format(args.server)
print(url)
response = requests.get(url)
print(response.text)

# both_between/<from_partition_key>/<to_partition_key>/<from_query>/<to_query>/<sort_mode>
print("both between")
url = "http://{}/both_between/people-100-2020-05/people-100-2020-07/friends-2019/friends-2020-06-~~/desc".format(args.server)
response = requests.get(url)
print(url)
print(response.text)