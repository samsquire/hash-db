# hash-db

You can create a basic database with a hash table and a prefix trie. You can create a basic distributed database with consistent hashing.

This demonstrates that use case.

[See this stackoverflow question](https://stackoverflow.com/questions/63420723/is-dynamodb-a-trie-in-front-of-a-distributed-hash-table)

[Also see the Java version here](https://github.com/samsquire/hash-db-java)

# API standard

## Sort key begins with value

http://localhost:1005/query_begins/people-100/messages/asc

## Pk begins and Sk begins

http://localhost:1005/query_pk_sk_begins/people/messages/desc

## Sort key between hash_values

http://localhost:1005/query_between/people-100/messages-101/messages-105/desc

## Partition key and sort key between values

http://localhost:1005/both_between/people-100-2020-05/people-100-2020-07/friends-2019/friends-2020-06-~~/desc
