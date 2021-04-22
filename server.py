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

from pprint import pprint
from operator import itemgetter
import re
from collections import defaultdict
from functools import reduce

hashes = {
    "hashes": None
}
indexed = {}
servers = []
sort_index = pygtrie.CharTrie()
between_index = {}
both_between_index = PartitionTree("", None)
partition_trees = {}
table_counts = {}

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
        partition_key, sort_key = lookup_key.split(":")
        print(partition_key)
        machine_index = hashes["hashes"].get_machine(partition_key) 

        if machine_index == new_index:
            print("Found key redistributed to this server") 
            # get the key from elsewhere in the cluster
            old_machine_index = old_hashes.get_machine(partition_key)
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
    machine_index = hashes["hashes"].get_machine(partition_key)
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
                machine_index = hashes["hashes"].get_machine(partition_key)
                server = servers[machine_index]

                response = requests.post("http://{}/get/{}".format(server, lookup_key))
                yield (sort_key, lookup_key, response.text)

    return Response(asstring(sorted(items(partition_key_query, query),
        key=lambda x: x[0], reverse=sort_mode == "desc")), mimetype="text/plain")


@app.route("/query_between/<partition_key>/<from_query>/<to_query>/<sort_mode>", methods=["GET"])
def query_between(partition_key, from_query, to_query, sort_mode):
    def items(partition_key, from_query, to_query):
        for partition_key, sort_key, lookup_key in between_index[partition_key].walk(from_query, to_query):
            machine_index = hashes["hashes"].get_machine(partition_key)
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
                machine_index = hashes["hashes"].get_machine(partition_key)
                server = servers[machine_index]

                response = requests.post("http://{}/get/{}".format(server, lookup_key))
                yield (sort_key, lookup_key, response.text)

    return Response(asstring(sorted(items(from_partition_key, to_partition_key, from_query, to_query),
        key=lambda x: x[0], reverse=sort_mode == "desc")), mimetype="text/plain")

class Parser():
    def __init__(self):
        self.last_char = " "
        self.pos = 0
        self.select_clause = []
        self.join_clause = []
        self.end = False
        self.group_by = None
        self.insert_fields = []
        self.insert_values = []
        self.where_clause = []
        self.fts_clause = []
        self.update_table = None 
        self.updates = []

    def getchar(self):
        
        char = self.statement[self.pos]
        if self.pos + 1 == len(self.statement):
            self.end = True
            return char
        self.pos = self.pos + 1
        
        return char
        
    def gettok(self):
        while (self.end == False and (self.last_char == " " or self.last_char == "\n")):
            self.last_char = self.getchar()
        
        
              
        if self.last_char == "(":
            self.last_char = self.getchar()
            return "openbracket"
        
        if self.last_char == ")":
            self.last_char = self.getchar()
            return "closebracket"
        
        if self.last_char == "*":
            self.last_char = self.getchar()
            return "wildcard"
        
        if self.last_char == "'":
            self.last_char = self.getchar()
            identifier = ""
            while self.end == False and self.last_char != "'":
                if self.last_char == "\\":
                    self.last_char = self.getchar()
                identifier = identifier + self.last_char
                self.last_char = self.getchar()
            if self.end and self.last_char != ")" and self.last_char != "'":
                identifier += self.last_char
            
            self.last_char = self.getchar()
            
            return identifier
        
        if re.match("[a-zA-Z0-9\.\_]+", self.last_char):
            identifier = ""
            while self.end == False and re.match("[a-zA-Z0-9\.\_]+", self.last_char):
                
                identifier = identifier + self.last_char
                self.last_char = self.getchar()
            
            if self.end and self.last_char != ")":
                identifier += self.last_char
            
            return identifier
    
        if self.last_char == "=":
            self.last_char = self.getchar()
            return "eq"
        
        if self.last_char == "~":
            self.last_char = self.getchar()
            return "tilde"
        
        if self.last_char == ",":
            self.last_char = self.getchar()
            return "comma"
        
        if self.end:
            return None
        
    
    def parse_select(self, token=None):
        if token == None:
            token = self.gettok()
        if token == "comma":
            self.parse_select()
        elif token == "from":
            self.table_name = self.gettok()
            self.parse_rest()
        elif token != None:
            identifier = token
            
            token = self.gettok()
            if token == "openbracket": # we're in a function
                function_parameters = self.gettok()
                if function_parameters == "wildcard":
                    function_parameters = "*"
            else:
                if identifier == "wildcard":
                    identifier = "*"
                self.select_clause.append(identifier)
                self.parse_select(token)
                return
            closebracket = self.gettok()
            
            self.select_clause.append(identifier + "(" + function_parameters + ")")
            self.parse_select()
    
    def parse_rest(self):
        operation = self.gettok()
        print(operation)
        if operation == None:
            return
        if operation == "group":
            by = self.gettok()
            group_by = self.gettok()
            self.group_by = group_by
            
        if operation == "inner":
            join = self.gettok()
            self.join_table = self.gettok()
            on = self.gettok()
            join_target_1 = self.gettok()
            
            self.gettok()
            join_target_2 = self.gettok()
            self.join_clause.append([join_target_1, join_target_2])
            self.parse_rest()
            
        if operation == "where":
            self.parse_where()
            
    def parse_where(self):
        field = self.gettok()
        equals = self.gettok()
        value = self.gettok()
        print(equals)
        print(value)
        if re.match("[0-9\.]+", value):
            value = int(value)
        print("Equals operator:" + equals)
        if equals == "eq":
            self.where_clause.append((field, value))
        if equals == "tilde":
            self.fts_clause.append((field, value))
        another = self.gettok()
        if another == "and":
            self.parse_where()
        
    
    def parse_insert_fields(self):
        field_name = self.gettok()
        self.insert_fields.append(field_name)
    
        token = self.gettok()
        if token == "closebracket":
            self.parse_rest_insert()
        if token == "comma":
            self.parse_insert_fields()
    
    def parse_values(self):
        value = self.gettok()
        if re.match("[0-9\.]+", value):
            value = int(value)
        self.insert_values.append(value)
       
        token = self.gettok()
        
        if token == "comma":
            self.parse_values()
        if token == "closebracket":
            print("We have finished parsing insert into")
            
        
    def parse_rest_insert(self):
        values = self.gettok()
        if values == "values":
            openbracket = self.gettok()
            self.parse_values()
    
    def parse_insert(self):
        self.insert_table = self.gettok()
        openbracket = self.gettok()
        
        if openbracket == "openbracket":
            self.parse_insert_fields()
    
    def parse(self, statement):
        self.statement = statement
        token = self.gettok()
        if token == "select":
            self.parse_select()
        if token == "insert":
            into = self.gettok()
            self.parse_insert()
        if token == "update":
            self.parse_update()

    def parse_setter(self):
        field = self.gettok()         
        operator = self.gettok()
        value = self.gettok()
        token = self.gettok()
        self.updates.append([field, value]) 
        if token == "set":
            self.parse_setter()
        if token == "where": 
            self.parse_where()

    def parse_update(self):
        self.update_table = self.gettok() 
        token = self.gettok()
        if token == "set": 
            self.parse_setter()
       

class SQLExecutor:
    def __init__(self, parser):
        self.parser = parser
    
    def get_tables(self, table_def):
        table_datas = []
        for pair in table_def:
            pair_data = []
            for selector in pair:
                table, field = selector.split(".")
                row_filter = "R.{}".format(table)
                table_data = list(filter(lambda x: x["key"].startswith(row_filter), items))
                pair_data.append((table_data, field))
            table_datas.append(pair_data)


        def table_reductions(table, metadata):
            for record in table:
                yield from reduce_table(metadata, record)
            yield metadata["current_record"]

        def reduce_table(table_metadata, record):
            components = record["key"].split(".")
            identifier = components[2]
            field_name = components[3]
            last_id = table_metadata["current_record"].get("internal_id")
            if last_id == None:
                table_metadata["current_record"] = {}
                table_metadata["current_record"]["internal_id"] = identifier
                table_metadata["current_record"][field_name] = record["value"]
            elif last_id != identifier:
                yield table_metadata["current_record"]
                # reset
                table_metadata["current_record"] = {}
                table_metadata["current_record"]["internal_id"] = identifier
                table_metadata["current_record"][field_name] = record["value"]
            elif last_id == identifier:
                table_metadata["current_record"][field_name] = record["value"]


        field_reductions = []
        for pair in table_datas:
            pair_items = []
            for item in pair:
                table, join_field = item
                field_reduction = table_reductions(table, defaultdict(dict))
                pair_items.append(field_reduction)
            field_reductions.append(pair_items)
        return table_datas, field_reductions
    
    def hash_join(self, records, index, pair, table_datas, process_records=True):
        ids_for_key = defaultdict(list)
        if process_records and len(records) > 0:
            scan = records
        else:
            scan = pair[0]
        
        for item in scan:
            field = table_datas[index][0][1]
            
            left_field = item[field]
            ids_for_key[left_field] = item
        
        for item in pair[1]:
            
            if table_datas[index][1][1] in item and item[table_datas[index][1][1]] in ids_for_key:
                item_value = item[table_datas[index][1][1]]
                print("Found match: {} in ids_for_key".format(item_value))
                yield {**ids_for_key[item[table_datas[index][1][1]]], **item}

    def get_table_size(self, table_name):
        if table_name not in table_counts:
            table_counts[table_name] = 0
        return table_counts[table_name]
         
    
    def execute(self):
        if self.parser.update_table:
            entries = []
            for server in servers:
                subset = json.loads(requests.post("http://{}/sql".format(server), data=json.dumps({ 
                    "parser": self.parser.__dict__
                    })).text)
                if subset:
                    entries = entries + subset
            print("From data node")
            print(entries)
            
        elif self.parser.fts_clause:
            entries = []
            for server in servers:
                subset = json.loads(requests.post("http://{}/sql".format(server), data=json.dumps({ 
                    "parser": self.parser.__dict__
                    })).text)
                if subset:
                    entries = entries + subset
            print("From data node")
            print(entries)
             
            
        elif self.parser.insert_values:
            insert_table = self.parser.insert_table
            print("Insert statement")
            created = False
            new_insert_count = 1
            for field, value in zip(self.parser.insert_fields, self.parser.insert_values):
                table_size = self.get_table_size(insert_table)
                if not created:
                    new_insert_count = table_size + 1
                table_counts[insert_table] = new_insert_count 
                items = []

                # create full text search index
                if isinstance(value, str): 
                    tokens = value.replace(",", "").split(" ")
                    for token in tokens:
                        new_key = "FTS.{}.{}.{}.{}".format(insert_table, field, token, new_insert_count)
                        items.append({
                            "key": new_key,
                            "value": new_insert_count
                        })
                
                
                new_key = "R.{}.{}.{}".format(insert_table, new_insert_count, field)
                items.append({
                    "key": new_key,
                    "value": value
                })
                new_key = "S.{}.{}.{}.{}".format(insert_table, field, value, new_insert_count)
                items.append({
                    "key": new_key,
                    "value": new_insert_count
                })
                new_key = "C.{}.{}.{}".format(insert_table, field, new_insert_count)
                items.append({
                    "key": new_key,
                    "value": value
                })
                if not created:
                    new_key = "R.{}.{}.id".format(insert_table, new_insert_count, field)
                    items.append({
                        "key": new_key,
                        "value": new_insert_count
                    })
                    created = True

                items.sort(key=itemgetter('key'))


                for item in items:
                    partition_key = "{}.{}".format(insert_table, new_insert_count)
                    sort_key = item["key"]
                    lookup_key = partition_key + ":" + sort_key
                    machine_index = hashes["hashes"].get_machine(partition_key)
                    response = requests.post("http://{}/set/{}/{}".format(servers[machine_index], partition_key, sort_key), data=str(item["value"]))

                    if lookup_key not in indexed:

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

            
        elif self.parser.group_by:
            print("Group by statement")
            group_by_components = parser.group_by.split(".")
            aggregator = defaultdict(list)
            row_specifier = "C.{}.{}".format(group_by_components[0], group_by_components[1])
            for item in filter(lambda x: x["key"].startswith(row_specifier), items):
                k = item["key"]
                v = item["value"]
      
                key_components = k.split(".")
                
                print(key_components[2])
                if (key_components[1] == group_by_components[0]) and (key_components[2] == group_by_components[1]):
                    aggregator[v].append(v)

            print(statement)
            for k, v in aggregator.items():
                output_line = ""
                for item in parser.select_clause:
                    if "count" in item:
                        output_line += str(len(aggregator[k]))
                    else:
                        output_line += str(k) + " "
                print(output_line)
                
        elif self.parser.join_clause:
            table_datas, field_reductions = self.get_tables(self.parser.join_clause)

            records = []
            for index, pair in enumerate(field_reductions):
                records = list(self.hash_join(records, index, pair, table_datas))
            
            records = self.process_wheres(records)
            print("records from join" + str(records))
            
            for record in records:
                output_line = []
                for clause in parser.select_clause:
                    table, field = clause.split(".")
                    output_line.append(record[field])
                print(output_line)
                
        elif self.parser.select_clause:
            entries = []
            for server in servers:
                subset = json.loads(requests.post("http://{}/sql".format(server), data=json.dumps({ 
                    "parser": self.parser.__dict__
                    })).text)
                entries = entries + subset
            print(entries)




    
    def process_wheres(self, field_reductions):
        where_clause = self.parser.where_clause
        fts_clause = self.parser.fts_clause
        data = list(field_reductions)
        records = []
        if not where_clause and not fts_clause:
            return field_reductions
        reductions = []
        table_datas = []
        and_or = []        
        
        for restriction, value in fts_clause:
            print("Running FTS search for where clause value " + str(value))
            table, field = restriction.split(".")
            tokens = value.split(" ")
            mode = "and"
            for token in tokens:
                if token == "&" or token == "|":
                    if token == "&":
                        mode = "and"
                    if token == "|":
                        mode = "or"
                    continue
                and_or.append(mode)
                row_filter = "FTS.{}.{}.{}".format(table, field, token)
                table_data = list(map(lambda x: {"id": x["value"]}, filter(lambda x: x["key"].startswith(row_filter), items)))
                
                reductions.append([data, table_data])
                table_datas.append([(data, "id"), (table_data, "id")])
        
        for restriction, value in where_clause:
            print("Running hash join for where clause value " + str(value))
            table, field = restriction.split(".")
            and_or.append("and")
            row_filter = "S.{}.{}.{}".format(table, field, value)
            table_data = list(map(lambda x: {"id": x["value"]}, filter(lambda x: x["key"].startswith(row_filter), items)))
            reductions.append([table_data, data])
            table_datas.append([(table_data, "id"), (data, "id")])
        
        process_records = True
        
        for index, pair in enumerate(reductions):
            if and_or[index] == "and":
                records = list(self.hash_join(records, index, pair, table_datas, process_records=True))
                
            if and_or[index] == "or":
                matched = list(self.hash_join(records, index, pair, table_datas, process_records=False))
                if matched:
                    records = records + matched
            print(records)
        
        
        
        return records

@app.route("/sql", methods=["POST"])
def sql():
    statement = json.loads(request.data)["sql"]
    parser = Parser()
    parser.parse(statement)
    SQLExecutor(parser).execute()
    return Response("")
