import random
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
import itertools
from cypher import CypherParser

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
joins = {}

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
    hashes["hashes"] = ConsistentHash(num_machines=len(servers), num_replicas=1)
    new_index = len(servers)  - 1
    bootstrapped_keys = {}
    print("Uploading missing data")
    for lookup_key in indexed.keys():
        partition_key, sort_key = lookup_key.split(":")
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
def set_value(partition_key, sort_key):
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

def object_exists(objects, key):
    return key in objects["object_key_lookup"]

def lookup_object_index(objects, key):
    if key in objects["object_key_lookup"]:
        return objects["object_key_lookup"][key]["index"]
    return None
    
def append_new_object(root, path, kind):
    print("appending to object")
    new_index = len(root["objects"])
    root["objects"].append({
        "index": new_index,
        "kind": kind,   
        "name": path
    })  
    root["object_key_lookup"][path] = {
        "index": new_index
    }

def create_path(path, key):
    return "{}.{}".format(path, key)

def create_list_path(path, key, index):
    return "{}.{}.{}".format(path, key, index)

def handle_object(root, key, value, path):
    print("handling object")
    if isinstance(value, dict):
        append_new_object(root, create_path(path, key), "dict")
        decouple_object(root, value)
        return "dict"
    if isinstance(value, str):
        print("found string")
        append_new_object(root, create_path(path, key), "string")
        return "string"
    if isinstance(value, int):
        print("found int")
        append_new_object(root, create_path(path, key), "int")
        return "int"
    if isinstance(value, list):
        print("found list")
        
        append_new_object(root, create_path(path, key), "list")
        for index, item in enumerate(value):
            new_kind = decouple_object(root, item, create_list_path(path, key, index))
            append_new_object(root, create_list_path(path, key, index), new_kind)
        return "list"

def decouple_object(root, document, path=""):
    if isinstance(document, dict):
        for key, value in document.items():
            handle_object(root, key, value, path)
        return "dict"

    elif isinstance(document, list):
        for value in document:
            handle_object(root, "", value, path)
        return "list"
                

def handle_create_keys_value(key, value, objects, path, parts, named_parts, is_list=False):
    if isinstance(value, str) or isinstance(value, int):
        print("path=", path)
        print("key=", key)
        lookup = "{}.{}".format(path, key)

        lookup = objects["object_key_lookup"][lookup] 
        index = lookup["index"]
        new_parts = list(parts) + [str(index)]
        if is_list:
            new_named_parts = "{}".format(named_parts)
        else:
            new_named_parts = "{}.{}".format(named_parts, key)
        yield {
            "named_key": new_named_parts,
            "key": ".".join(new_parts),
            "value": value
        }
    if isinstance(value, dict):
        print("path=", path)
        print("key=", key)
        lookup = "{}".format(path)
        lookup = objects["object_key_lookup"][lookup]
        index = lookup["index"]
        new_parts = list(parts) + [str(index)]
        if is_list:
            new_named_parts = "{}".format(named_parts)
        else:
            new_named_parts = "{}.{}".format(named_parts, key)
        yield from create_keys(value, objects, path="{}.{}".format(path, key), parts=new_parts, named_parts=new_named_parts)
    
    if isinstance(value, list):
        print("path=", path)
        print("key=", key)
        for item_index, item in enumerate(value): 
            lookup = objects["object_key_lookup"]["{}.{}".format(path, key)]
            index = lookup["index"]
            new_parts = list(parts) + [str(item_index)]
            yield from handle_create_keys_value(item_index, item, objects, path="{}.{}".format(path, key), parts=new_parts, named_parts=".{}.{}[]".format(named_parts, key), is_list=True)

def create_keys(document, objects, path="", parts=[], named_parts=""):
    for key, value in document.items(): 
        yield from handle_create_keys_value(key, value, objects, path, parts, named_parts, is_list=False)
                 

     
    
@app.route("/documents/<collection>/<identifier>", methods=["GET"])
def get_document(collection, identifier):
    print("getting document")
    machine_index = hashes["hashes"].get_machine(identifier)
    server = servers[machine_index]
    response = requests.get("http://{}/documents/{}/{}".format(server, collection, identifier))
    print(response.text)

    return response.text
     

@app.route("/save/<collection>/<identifier>", methods=["POST"])
def save_document(collection, identifier):
    print("json storage server")
    document = json.loads(request.data)
    machine_index = hashes["hashes"].get_machine(identifier)
    server = servers[machine_index]
    lookup_key = "documents.objects"
    response = requests.post("http://{}/get/{}".format(server, lookup_key))
    print(response.text)
    if not response.text:   
        objects = {
            "objects": [],
            "object_key_lookup": {}
        }
    else:
        objects = response.json()
    object_count = len(objects["objects"])
    decouple_object(objects, document)  
    partition_key = "DDB"
    sort_key = lookup_key   

    print(objects)  
    response = requests.post("http://{}/set/{}/{}".format(server, partition_key, sort_key), data=json.dumps(objects))
    created = list(create_keys(document, objects))
    print("Keys for save")
    pprint(created)
    def items():
        for value in created:
            print("Saving document key to database")
            partition_key = "D.{}.{}".format(collection, identifier)
            sort_key = value["key"][1:]
            parser = Parser()
            parser.insert_table = collection
            parser.insert_fields = [value["named_key"][1:].replace(".", "~")]
            print(parser.insert_fields)
            parser.insert_values = [value["value"]]
            # response = requests.post("http://{}/set/{}/{}".format(server, partition_key, sort_key), data=str(value["value"]))

            
            yield from SQLExecutor(parser).execute()
     
    response = requests.post("http://{}/save/{}/{}".format(server, collection, identifier), data=json.dumps(created))
    return Response(json.dumps(list(items())))

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
        self.create_join_clause = []
        self.create_join_source = None

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
        
        if re.match("[a-zA-Z0-9\.\_\[\]\/\~]+", self.last_char):
            identifier = ""
            while self.end == False and re.match("[a-zA-Z0-9\.\_\[\]\/\~]+", self.last_char):
                
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
    
    def parse_create_join(self):
       token = self.gettok() 

       if token == "inner":
           join = self.gettok()
           self.create_join_source = self.gettok() # never actually used
           on = self.gettok()
           join_target_1 = self.gettok()
           
           self.gettok()
           join_target_2 = self.gettok()
           self.create_join_clause.append([join_target_1, join_target_2])
           self.parse_create_join()

    def parse_create(self):
        token = self.gettok() 
        if token == "join":
            self.parse_create_join()

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
        if token == "create":
            self.parse_create()

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
        lhs = 0

        scan = None
        
        for innerindex, entry in enumerate(table_datas[index]):
            collection, fieldname, size = entry
            if size == "smaller":
                lhs = innerindex
                scan = collection
                break 

                
        for item in itertools.chain(scan, records):
            field = table_datas[index][lhs][1]
            if field not in item:
                continue
            left_field = item[field]
            ids_for_key[left_field] = item
        
        test = None
        rhs = 1
        for rhsindex, entry in enumerate(table_datas[index]):
            collection, fieldname, size = entry
            if collection is not scan:
                rhs = rhsindex
                test = collection
                break

        try:
            for item in test:
                
                if table_datas[index][rhs][1] in item and item[table_datas[index][rhs][1]] in ids_for_key:
                    item_value = item[table_datas[index][rhs][1]]
                    print("Found match: {} in ids_for_key".format(item_value))
                    yield {**ids_for_key[item[table_datas[index][rhs][1]]], **item}

        except KeyError:
            pass

    def get_table_size(self, table_name):
        if table_name not in table_counts:
            table_counts[table_name] = 0
        return table_counts[table_name]
         
    
    def execute(self):
        print("Executing SQL statement")
        if self.parser.create_join_clause: 
            print("Creating a join")
            print(self.parser.create_join_clause)
            for clause in self.parser.create_join_clause: 
                left_table, left_field = clause[0].split(".") 
                right_table, right_field = clause[1].split(".") 
                print(left_table)
                print(right_table)
                if left_table in joins:
                    joins[left_table].append({"clause": clause})
                else:
                    joins[left_table] = [{"clause": clause}]
                if right_table in joins:
                    joins[right_table].append({"clause": clause})
                else:
                    joins[right_table] = [{"clause": clause}]

            print(joins)

                # if you insert into left table, you also need to insert join targets into right table  
                # i need to do a select right.id from right_table where left_table.left_field = right_table.right_field
                # i need to insert left_field onto all servers that return a right id
                 

        elif self.parser.update_table:
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
            for server in servers:
                subset = json.loads(requests.post("http://{}/sql".format(server), data=json.dumps({ 
                    "parser": self.parser.__dict__
                    })).text)
                yield from subset

             
            
        elif self.parser.insert_values:
            insert_table = self.parser.insert_table
            print("Insert statement")
            created = False
            new_insert_count = 1
            for field, value in zip(self.parser.insert_fields, self.parser.insert_values):
                all_servers = []
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
                    new_key = "R.{}.{}.id".format(insert_table, new_insert_count)
                    new_id = {
                        "key": new_key,
                        "value": new_insert_count
                    }
                    items.append(new_id)
                    created = True
                    all_servers.append(new_id)

                    new_key = "S.{}.{}.{}.{}".format(insert_table, "id", new_insert_count, new_insert_count)
                    items.append({
                        "key": new_key,
                        "value": new_insert_count
                    })

                items.sort(key=itemgetter('key'))
                # for item in all_servers:
                #    for server in servers:
                #       partition_key = "{}.{}".format(insert_table, new_insert_count)
                #        sort_key = item["key"]
                #        lookup_key = partition_key + ":" + sort_key
                #        response = requests.post("http://{}/set/{}/{}".format(server, partition_key, sort_key), data=str(item["value"]))
                pprint(items)
                for item in items:
                    partition_key = "{}.{}".format(insert_table, new_insert_count)
                    sort_key = item["key"]
                    lookup_key = partition_key + ":" + sort_key
                    machine_index = hashes["hashes"].get_machine(partition_key)
                    url = "http://{}/set/{}/{}".format(servers[machine_index], partition_key, sort_key)
                    print(url)
                    print(str(item["value"]))
                    response = requests.post(url, data=str(item["value"]))

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

               # we need to check if any materialized joins 
                if insert_table in joins:
                    join_clauses = joins[insert_table]
                    print(join_clauses)
                    for join_clause in join_clauses:
                        clauses = join_clause["clause"]
                        left_components = clauses[0].split(".")
                        left_table = left_components[0]
                        left_field = left_components[1]

                        right_components = clauses[1].split(".")
                        right_table = right_components[0]
                        right_field = right_components[1]

                        if right_table == insert_table:
                           print("We need to swap") 
                           temp_table = right_table
                           temp_field = right_field
                           right_table = left_table
                           right_field = left_field
                           left_table = temp_table 
                           left_field = temp_field


                        print("Do we need to join this inserted data?")
                        print(field)
                        print(left_field)
                        search_value = value
                        if left_field == "id":
                            search_value = str(new_insert_count)
                        
                        if field == left_field or left_field == "id":
                        
                            # Do the prejoin
                            parser = Parser()
                            statement = "select {}.id, {}.{} from {} where {}.{} = {}".format(
                                        right_table,
                                        right_table, right_field,
                                        right_table,
                                        right_table,
                                        right_field,
                                        search_value)
                            parser.parse(statement)
                            print(statement)
                            data = SQLExecutor(parser).execute()
                            for match in data:
                                # Remember left table is the join key
                                # Right table is the table we're trying to insert data into
                                server_value = match[1]
                                print("{} {}".format(left_table, right_table))
                                partition_key = "{}.{}".format(right_table, server_value)

                                # first we insert data where it belongs
                                partition_key = "{}.{}".format(left_table, new_insert_count)
                                machine_index = hashes["hashes"].get_machine(partition_key)
                                server = servers[machine_index] 
                                
                                print("Data from {}, we are inserting {} into server {}".format(server, server_value, servers[machine_index]))
                                response = requests.post("http://{}/set/{}/R.{}.{}.{}".format(
                                        server,
                                        partition_key,
                                        right_table,
                                        server_value,
                                        right_field), data=str(search_value)) 
                                response = requests.post("http://{}/set/{}/R.{}.{}.{}".format(
                                        server,
                                        partition_key,
                                        right_table,
                                        server_value,
                                        "id"), data=server_value) 

                                # Insert the data on the joined servers
                                server = match[0]
                                print(server)
                                print("Data from {}, we are inserting {} into server {}".format(server, server_value, servers[machine_index]))
                                # new_key = "R.{}.{}.{}".format(insert_table, new_insert_count, field)
                                # response = requests.post("http://{}/set/{}/R.{}.{}.{}".format(
                                #         server,
                                #         partition_key,
                                #         left_table,
                                #         new_insert_count,
                                #         left_field), data=str(search_value)) 
                                # response = requests.post("http://{}/set/{}/R.{}.{}.{}".format(
                                #         server,
                                #         partition_key,
                                #         left_table,
                                #         new_insert_count,
                                #         "id"), data=str(new_insert_count)) 
                                # have to create a key on 

            
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
            records = []
            # server = random.choice(servers)
            for server in servers:
                records.extend(json.loads(requests.post("http://{}/sql".format(server), data=json.dumps({ 
                    "parser": self.parser.__dict__
                    })).text))


            print(records)

            missing_fields = set()
            missing_records = []
            for record in records:
                if record["missing_fields"]:
                    missing_fields = missing_fields.union(set(record["missing_fields"]))
                    for dataitem in record["outputs"]:
                        missing_records.append(dataitem)
            print("Missing fields:")
            print(missing_fields) 
            missing_index = {}

            for index, missing_record in enumerate(missing_records): 
                missing_index[str(index)] = missing_record
                missing_record["missing_index"] = str(index)

            def trim_record(join_fields, items):
                for item in items:
                    data = {
                            "missing_index": item["missing_index"],
                            "id": item["id"]
                    }
                    for join_field in join_fields:
                        data[join_field] = item[join_field]
                    yield data

            join_fields = []
            join_specs = []

            for missing_field in missing_fields:
                for select_clause in self.parser.select_clause:
                    select_table, select_field = select_clause.split(".")
                    if select_field == missing_field:
                        for join_clause in self.parser.join_clause:
                            left_components = join_clause[0].split(".")
                            left_table = left_components[0]
                            left_field = left_components[1]
                            right_components = join_clause[1].split(".")
                            right_table = right_components[0]
                            right_field = right_components[1]

                            id_field = None 
                            if select_table == left_table:  
                                id_field = "id"     
                                join_field = "{}_{}".format(left_table, "id")
                                print("Join field -> {}".format(join_field))
                                 
                            elif select_table == right_table:  
                                id_field = "id"     
                                join_field = "{}_{}".format(right_table, "id")
                                print("Join field -> {}".format(join_field))

                            if not id_field:
                                print(select_table) 
                                print(left_table) 
                                print(right_table) 
                                continue
                            
                            print("select {} from {} inner join {} on {} = {}".format(
                                missing_field, "network_table", select_table, id_field, join_field))  


                            join_fields.append(join_field)

                            join_specs.append({ 
                                    "id_field": id_field,
                                    "join_field": join_field,
                                    "missing_field": missing_field,
                                    "select_table": select_table
                                    })


            valid_matches = list(trim_record(join_fields, filter(lambda x: join_field in x, missing_records)))
            print("Valid matches")
            pprint(valid_matches)

            def getresults(server):
                response = json.loads(requests.post("http://{}/networkjoin".format(server), data=json.dumps({
                    
                    "parser": self.parser.__dict__,
                    "join_specs": join_specs,
                    "records": valid_matches
                    })).text)

                yield response          

            with ThreadPoolExecutor(max_workers=len(servers)) as executor:
                future = executor.map(getresults, servers)
                print("Doing network join...")
                for server in future:
                    for rowset in server:
                        print("Missing data records")
                        print(len(missing_records))
                        pprint(missing_records)
                        for missing_data in rowset:
                            print("Missing data")
                            print(missing_data)
                            missing_field, missing_index_key, found_data = missing_data
                            if missing_data:
                                missing_index[missing_index_key][missing_field] = found_data
                
                    

            outputs = []
            for item in records:
                for obj in item["outputs"]:
                    outputs.append(obj)
            # here                
            header = ""
            output_lines = []
            have_printed_header = False
            for result in outputs:
                skip = False
                output_line = []
                for field in self.parser.select_clause:

                    if field == "*":
                        for key, value in result.items():
                            if not have_printed_header:
                                header.append(key)
                            output_line.append(value)
                    else:
                        table, field_name = field.split(".")
                        if field_name not in result:
                            skip = True
                        else:
                            output_line.append(result[field_name])
                if skip:
                    continue
                output_lines.append(output_line)
                have_printed_header = True
            print(header)
            yield from output_lines
                        
                
        elif self.parser.select_clause:
            for server in servers:
                subset = json.loads(requests.post("http://{}/sql".format(server), data=json.dumps({ 
                    "parser": self.parser.__dict__
                    })).text)
                for result in subset:
                    item = [server] + result
                    yield item                  




    
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
    print(statement)
    def items():
        yield from SQLExecutor(parser).execute()
     
    return Response(json.dumps(list(items())))

class CypherExecutor:
    def __init__(self, parser):
        self.parser = parser

    def execute(self, server_key):
        machine_index = hashes["hashes"].get_machine(server_key)
        subset = json.loads(requests.post("http://{}/cypher".format(servers[machine_index]), data=json.dumps({ 
            "parser": self.parser.__dict__
            })).text)
        yield from subset
         
         

@app.route("/cypher", methods=["POST"])
def cypher():
    request_data = json.loads(request.data)
    statement = request_data["cypher"]
    server_key = request_data["key"]
    parser = CypherParser()
    parser.parse(statement)
    print(statement)
    def items():
        yield from CypherExecutor(parser).execute(server_key)
     
    return Response(json.dumps(list(items())))
