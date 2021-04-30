# hash-db

You can create a basic database with a hash table and a prefix trie. See hash-db.py It's a very small database. This reflects dynamodb style querying. You can create a basic distributed database with consistent hashing. See client.py and server.py. Data is rebalanced onto nodes as new servers are added. SQL is parsed on the server and work distributed to data nodes. [Please see this blog post](https://elaeis.cloud-angle.com/?p=183). For how distributed join works: [see this blog post](https://elaeis.cloud-angle.com/?p=192).

This project demonstrates how simple a database can be. Do not use for serious data, it's only stored in memory and there is no persistence.

[See this stackoverflow question](https://stackoverflow.com/questions/63420723/is-dynamodb-a-trie-in-front-of-a-distributed-hash-table)

[Also see the Java version here](https://github.com/samsquire/hash-db-java)

This project uses Google's pygtrie and [Michaeln Nielsen's consistent hashing code](michaelnielsen.org/blog/consistent-hashing/) 

# Running

Run ./start-all.sh to start server with 3 data nodes.

# Distributed joins

Data is distributed across the cluster, with rows being on one server each. I haven't gotten around to load balancing the data.

First, register a join with the server:

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

# API standard

## Sort key begins with value

http://localhost:1005/query_begins/people-100/messages/asc

## Pk begins and Sk begins

http://localhost:1005/query_pk_sk_begins/people/messages/desc

## Sort key between hash_values

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
