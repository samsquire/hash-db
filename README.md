# hash-db

This is an experimental project.

* You can create a basic database with a hash table and a prefix trie. See hash-db.py It's a very small database.
* This reflects dynamodb style querying.
* You can create a basic distributed database with consistent hashing. See client.py and server.py. Data is rebalanced onto nodes as new servers are added.
* SQL is parsed on the server and work distributed to data nodes. Data nodes store a subset of the data in the database.
* Cypher is parsed on the server and distributed to a data node for processing. Graphs only live on one data node at a time. I haven't worked out how to distribute their processing yet.

This project demonstrates how simple a database can be. Do not use for serious data, it's only stored in memory and there is no persistence.

[See this stackoverflow question](https://stackoverflow.com/questions/63420723/is-dynamodb-a-trie-in-front-of-a-distributed-hash-table)

[Also see the Java version here](https://github.com/samsquire/hash-db-java)

This project uses Google's pygtrie and [Michaeln Nielsen's consistent hashing code](michaelnielsen.org/blog/consistent-hashing/) 

This project uses [converged indexes as designed by Rockset](https://rockset.com/blog/converged-indexing-the-secret-sauce-behind-rocksets-fast-queries/).

# Running

Run `pip install -r requirements.txt`

Run ./start-all.sh to start server with 3 data nodes. See example.py for the tests that I run as part of development.

# Distributed joins

Data is distributed across the cluster, with rows being on one server each. Join keys are inserted on matching records upon insert. It's not very efficient as it does the join on every insert to maintain the join. I plan to keep joined data together at all times. I haven't gotten around to load balancing the data.

First, register a join with the server:

```
create join
    inner join people on people.id = items.people
    inner join products on items.search = products.name

```

```
print("create join sql")
statement = """create join
    inner join people on people.id = items.people
    inner join products on items.search = products.name
    """
url = "http://{}/sql".format(args.server)
response = requests.post(url, data=json.dumps({
    "sql": statement
    }))
print(url)
print(response.text)

```

Insert data. The join is maintained as you insert data. Data is spread out across the cluster.

In join on one server and then ask for missing data from the other data nodes.

```
curl -H"Content-type: application/json" -X POST http://localhost:1005/sql --data-ascii '{"sql": "select products.price, people.people_name, items.search from items inner join people on items.people = people.id inner join products on items.search = products.name"}'

```

# DynamoDB API standard

## Sort key begins with value

http://localhost:1005/query_begins/people-100/messages/asc

## Pk begins and Sk begins

http://localhost:1005/query_pk_sk_begins/people/messages/desc

## Sort key between values

http://localhost:1005/query_between/people-100/messages-101/messages-105/desc

## Partition key and sort key between values

http://localhost:1005/both_between/people-100-2020-05/people-100-2020-07/friends-2019/friends-2020-06-~~/desc

## SQL Interface

```
curl -H"Content-type: application/json" -X POST http://localhost:1005/sql --data-ascii '{"sql": "select * from people"}'
```

```
print("4 insert sql")                                                                                                  |            elif last_id == identifier:                                                                               
url = "http://{}/sql".format(args.server)                                                                              |                table_metadata["current_record"][field_name] = data[lookup_key]                                       
response = requests.post(url, data=json.dumps({                                                                        |                                                                                                                      
    "sql": "insert into people (people_name, age) values ('Sam', 29)"                                                  |                                                                                                                      
    }))                                                                                                                |        field_reductions = []                                                                                         
print(url)                                                                                                             |        for index, pair in enumerate(table_datas):                                                                    
print(response.text) 
```

## Cypher interface

For simplicity, we only support Cypher triples. That is, (node)-[:relationship]-(node) separated by commas. But the sum of the triples can produce the same output as if the Cypher was all in one line.

```
curl -H"Content-type: application/json" -X POST http://localhost:1005/cypher --data-ascii '{"key": "1", "cypher": "match (start:Person)-[:FRIEND]->(end:Person), (start)-[:LIKES]->(post:Post), (end)-[:POSTED]->(post) return start, end, post"}'
```

```
query = """match (start:Person)-[:FRIEND]->(end:Person),
 (start)-[:LIKES]->(post:Post), 
(end)-[:POSTED]->(post)
 return start, end, post"""
print(query)
url = "http://{}/cypher".format(args.server)
response = requests.post(url, data=json.dumps({
    "key": "1",
    "cypher": query
    }))
print(url)
print(response.text)

```

# How SQL is executed

Once SQL is parsed, the query and join is turned into a stream of operators. The underlying storage is a keyvalue database or HashMap. Eventually if we implement physic storage this would be a keyvalue backend such as RocksDB which provides efficient iterators of which we need range scans for this database to be efficient. We use a [rockset converged index](https://rockset.com/blog/converged-indexing-the-secret-sauce-behind-rocksets-fast-queries/).

Keyvalues are stored for each column of a table. There is no create table statement.

For example the table people (first_name, age, introduction) is stored as the following keyvalues -

```
R.people.0.name
R.people.0.age
R.people.0.introduction
C.people.name.0
C.people.age.0
C.people.introduction.0
```

Based on the joins that need to be done, we create a list of join operators.

To execute a query we take two stream operators at a time and join them together on a key 
