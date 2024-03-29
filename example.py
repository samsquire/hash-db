import requests
import json

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

print("create join sql")
statement = """create join
    inner join people on people.id = items.people
    inner join products on items.search = products.name
    inner join reviews on items.search = reviews.product
    """
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": statement 
    }))
print(url)
print(response.text)

print("1 insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "insert into people (people_name, age) values ('Ted', 29)" 
    }))
print(url)
print(response.text)

print("2 insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "insert into people (people_name, age) values ('Fred', 45)" 
    }))
print(url)
print(response.text)

print("3 insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "insert into people (people_name, age) values ('Simon', 29)" 
    }))
print(url)
print(response.text)

print("4 insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "insert into people (people_name, age) values ('Sam', 29)" 
    }))
print(url)
print(response.text)

print("query sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "select * from people where people.age = 29" 
    }))
print(url)
print(response.text)

print("insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "update people set people.age = 31 where people.people_name = 'Sam'" 
    }))
print(url)
print(response.text)


print("query sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "select * from people where people.age = 29" 
    }))
print(url)
print(response.text)

print("query sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "select * from people where people.age = 31" 
    }))
print(url)
print(response.text)

print("insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "insert into items (search, people) values ('Cat', 3)" 
    }))
print(url)
print(response.text)


print("insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "insert into products (name, price) values ('Spanner', 300)" 
    }))
print(url)
print(response.text)

print("insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "insert into products (name, price) values ('Tree', 1000)" 
    }))
print(url)
print(response.text)

print("insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "insert into products (name, price) values ('Spanner', 450)" 
    }))
print(url)
print(response.text)


print("insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "insert into products (name, price) values ('Spanner', 600)" 
    }))
print(url)
print(response.text)


print("insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "insert into items (search, people) values ('Spanner', 3)" 
    }))
print(url)
print(response.text)

print("insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "insert into items (search, people) values ('Tree', 3)" 
    }))
print(url)
print(response.text)

print("insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "insert into items (search, people) values ('Spanner', 2)" 
    }))
print(url)
print(response.text)

print("insert sql")
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": "insert into reviews (score, product) values ('5', 'Spanner')" 
    }))
print(url)
print(response.text)

print("query sql")
statement = """select products.price, people.people_name,
    items.search from items
    inner join people on items.people = people.id
    inner join products on items.search = products.name
    """
url = "http://{}/sql".format(args.server)
print(statement)
response = requests.post(url, data=json.dumps({
    "sql": statement 
    }))
print(url)
print(response.text)


queries = [
"""merge (person:Person {'name': 'Samuel'})-[:FRIEND]->(tasya:Person {'name': 'Tasya'})""",
"""merge (person:Person {'name': 'Tasya'})-[:FRIEND]->(obj:Person {'name': 'Samuel'})""",
"""merge (person:Person {'name': 'Samuel'})-[:FRIEND]->(obj:Person {'name': 'Simon'})""",
"""merge (person:Person {'name': 'Simon'})-[:FRIEND]->(obj:Person {'name': 'Samuel'})""",
"""merge (person:Person {'name': 'Samuel'})-[:FRIEND]->(obj:Person {'name': 'John'})""",
"""merge (person:Person {'name': 'Simon'})-[:FRIEND]->(obj:Person {'name': 'Sally'})""",
"""merge (person:Person {'name': 'Sally'})-[:FRIEND]->(obj:Person {'name': 'Simon'})""",
"""merge (person:Person {'name': 'Tasya'})-[:FRIEND]->(obj:Person {'name': 'Margaret'})""",
"""merge (person:Person {'name': 'Margaret'})-[:FRIEND]->(obj:Person {'name': 'Tasya'})""",
"""merge (person:Person {'name': 'Samuel'})-[:LIKES]->(obj:Post {'name': 'Ideas'})""",
"""merge (person:Person {'name': 'Tasya'})-[:POSTED]->(obj:Post {'name': 'Ideas'})""",
"""merge (person:Person {'name': 'Tasya'})-[:POSTED]->(obj:Post {'name': 'Lamentations'})""",
"""merge (person:Person {'name': 'Tasya'})-[:POSTED]->(obj:Post {'name': 'Love'})""",
"""merge (person:Person {'name': 'Tasya'})-[:POSTED]->(obj:Post {'name': 'Thoughts'})""",
"""merge (person:Person {'name': 'Samuel'})-[:LIKES]->(obj:Post {'name': 'Thoughts'})""",
"""merge (person:Person {'name': 'Tasya'})-[:LIKES]->(obj:Food {'name': 'Pocky'})""",
"""merge (person:Post {'name': 'Ideas'})-[:REFERS]->(obj:Person {'name': 'Margaret'})""",
"""merge (person:Post {'name': 'Thoughts'})-[:REFERS]->(obj:Person {'name': 'John'})""",
"""merge (person:Post {'name': 'Samuel'})-[:LIKES]->(obj:Post {'name': 'Love'})""",
]

for query in queries:
    print(query)
    url = "http://{}/cypher".format(args.server)
    response = requests.post(url, data=json.dumps({
        "key": "1",
        "cypher": query 
        }))
    print(url)
    print(response.text)

query = """match (start:Person)-[:FRIEND]->(end:Person), (start)-[:LIKES]->(post:Post), (end)-[:POSTED]->(post:Post), (post:Post)-[:REFERS]->(person:Person) return start, end, post, person"""
print(query)
url = "http://{}/cypher".format(args.server)
response = requests.post(url, data=json.dumps({
    "key": "1",
    "cypher": query 
    }))
print(url)
print(response.text)


print("query sql")
statement = """
insert into items (search, people) values ('blah sentence', 3)
"""
url = "http://{}/sql".format(args.server)
print(statement)
response = requests.post(url, data=json.dumps({
    "sql": statement 
    }))
print(url)
print(response.text)

print("query sql")
statement = """
select * from items where items.search ~ 'blah | nonsense | notthere' and items.people = 3
"""
url = "http://{}/sql".format(args.server)
print(statement)
response = requests.post(url, data=json.dumps({
    "sql": statement 
    }))
print(url)
print(response.text)

print("json storage")
url = "http://{}/save/people/1".format(args.server)
response = requests.post(url, data=json.dumps({
    "name": "Sam Squire", 
	"age": 32,
	"hobbies": [{"name": "God"}, {"name": "databases"}, {"name": "computers"}]
    }))
print(response.text)

print("json retrieve")
url = "http://{}/documents/people/1".format(args.server)
response = requests.get(url)
print(response.text)



print("query documents by sql")
statement = """
select * from people where people.~hobbies[]~name = 'God'"""
url = "http://{}/sql".format(args.server)
print(statement)
response = requests.post(url, data=json.dumps({
    "sql": statement 
    }))
print(url)
print(response.text)

print("query multiple hobbies documents by sql")
statement = """
select people.~hobbies[]~name from people"""
url = "http://{}/sql".format(args.server)
print(statement)
response = requests.post(url, data=json.dumps({
    "sql": statement 
    }))
print(url)
print(response.text)

