import os
import json
import requests
from argparse import ArgumentParser
from flask import Flask, request, make_response, Response
from datastructures import Tree, PartitionTree
import pygtrie
from consistent_hashing import ConsistentHash
from pprint import pprint
from collections import defaultdict

parser = ArgumentParser()
parser.add_argument("--server")
parser.add_argument("--port")
args = parser.parse_args(os.environ["HASHDB_ARGS"].split(" "))

self_server = "localhost" + args.port

data = {}
indexed = {}
servers = []
sort_index = pygtrie.CharTrie()
between_index = {}
both_between_index = PartitionTree("", None)
partition_trees = {}

sql_index = pygtrie.CharTrie()

response = requests.post("http://{}/bootstrap/{}".format(args.server, args.port))
print(response.text)
bootstrapped_keys = json.loads(response.text)
for key, value in bootstrapped_keys.items():
    data[key] = value

app = Flask(__name__)


@app.route("/get/<lookup_key>", methods=["POST"])
def get(lookup_key):
    return make_response(str(data[lookup_key]))

@app.route("/set/<partition_key>/<sort_key>", methods=["POST"])
def set(partition_key, sort_key):

    
    lookup_key = partition_key + ":" + sort_key
    data[lookup_key] = request.data.decode('utf-8')
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

    sql_index[sort_key] = lookup_key

    return make_response('', 202)

@app.route("/clear/<lookup_key>", methods=["POST"])
def clear(lookup_key):
    del data[lookup_key]
    return make_response('', 202)

@app.route("/dump")
def dump():
    return make_response(json.dumps(data), 202)

@app.route("/ping", methods=["POST"])
def ping():
    length = len(data)
    return make_response(str(length), 202)

@app.route("/query_begins/<partition_key>/<query>", methods=["POST"])
def query_begins(partition_key, query):
    data = json.loads(request.data)
    hashes = ConsistentHash.from_dict(data["hashes"])
    servers = data["servers"]

    def items():
        try:
            for lookup_key, sort_key in sort_index.iteritems(prefix=partition_key + ":" + query):
                machine_index = hashes.get_machine(partition_key)
                server = servers[machine_index]

                if self_server == server:
                    yield {"sort_key": sort_key, "lookup_key": lookup_key, "value": data[lookup_key]}
                else:
                    response = requests.post("http://{}/get/{}".format(server, lookup_key))
                    yield {"sort_key": sort_key, "lookup_key": lookup_key, "value": response.text}
        except:
            print("No keys match on this server")
    return Response(json.dumps(list(items())), mimetype="text/plain")

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
                try:
                    table_data = sql_index.iteritems(prefix=row_filter)
                except:
                    table_data = []
                pair_data.append((table_data, field))
            table_datas.append(pair_data)


        def table_reductions(table, metadata):
            try:
                for record in table:
                    yield from reduce_table(metadata, record)
                yield metadata["current_record"]
            except:
                yield []
                yield metadata["current_record"]

        def reduce_table(table_metadata, record):
            sort_key, lookup_key = record
            components = sort_key.split(".")
            identifier = components[2]
            field_name = components[3]
            last_id = table_metadata["current_record"].get("internal_id")
            if last_id == None:
                table_metadata["current_record"] = {}
                table_metadata["current_record"]["internal_id"] = identifier
                table_metadata["current_record"][field_name] = data[lookup_key]
            elif last_id != identifier:
                yield table_metadata["current_record"]
                # reset
                table_metadata["current_record"] = {}
                table_metadata["current_record"]["internal_id"] = identifier
                table_metadata["current_record"][field_name] = data[lookup_key]
            elif last_id == identifier:
                table_metadata["current_record"][field_name] = data[lookup_key]


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
        if self.parser["updates"]:
            table_datas, field_reductions = self.get_tables([["{}.".format(self.parser["update_table"])]])
            for result in self.process_wheres(field_reductions[0][0]):
                for update in self.parser["updates"]:
                    updated_to, new_value = update
                    updated_field = updated_to.split(".")[1]
                    partition_key = "{}.{}".format(self.parser["update_table"], result["id"])
                    items_to_be_deleted = [ 
                            ]

                    new_key = "R.{}.{}.{}".format(self.parser["update_table"], result["id"], updated_field)
                    items_to_be_deleted.append(new_key)
                    new_key = "S.{}.{}.{}.{}".format(self.parser["update_table"], updated_field, result[updated_field], result["id"])
                    items_to_be_deleted.append(new_key)
                    new_key = "C.{}.{}.{}".format(self.parser["update_table"], updated_field, result["id"])
                    items_to_be_deleted.append(new_key)

                    if isinstance(result[updated_field], str):
                        tokens = result[updated_field]
                        print("Need to delete FTS indexes")
                        for token in tokens:
                            new_key = "FTS.{}.{}.{}.{}".format(self.parser["update_table"], updated_field, token, result["id"])
                            items_to_be_deleted.append(new_key)

                    for item in items_to_be_deleted:
                        sort_key = item
                        lookup_key = partition_key + ":" + sort_key
                        try:
                            del sort_index[lookup_key]
                            del sort_index[sort_key][partition_key]
                            del sort_index[sort_key]
                            # if partition_key not in between_index:
                            #     between_index[partition_key] = Tree("", None, None)
                            between_index[partition_key].delete(sort_key)
                            if partition_key not in partition_trees:
                                partition_tree = both_between_index.insert(partition_key, Tree("", None, None))
                                partition_trees[partition_key] = partition_tree
                            # between_index[partition_key].insert(sort_key, partition_key, partition_key + ":" + sort_key)
                            # partition_trees[partition_key].partition_tree.insert(sort_key, partition_key, partition_key + ":" + sort_key)

                            del sql_index[sort_key]
                            del data[lookup_key]
                        except KeyError:
                            pass
                
            
        elif self.parser["fts_clause"]:
            # full text search
            table_datas, field_reductions = self.get_tables([["{}.".format(self.parser["table_name"])]])
            have_printed_header = False
            header = []
            output_lines = []
            outputs = []
            for result in self.process_wheres(field_reductions[0][0]):
                print("item: " + str(result))
                output_lines = []
                for field in self.parser["select_clause"]:

                    if field == "*":
                        for key, value in result.items():
                            if not have_printed_header:
                                header.append(key)
                            output_lines.append(value)
                    else:
                        output_lines.append(result[field])
                have_printed_header = True
                outputs.append(output_lines)
            print(header)
            print(outputs)
            yield output_lines
            
        elif self.parser["group_by"]:
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
                
        elif self.parser["join_clause"]:
            table_datas, field_reductions = self.get_tables(self.parser.join_clause)

            records = []
            for index, pair in enumerate(field_reductions):
                records = self.hash_join(records, index, pair, table_datas)
            
            records = self.process_wheres(records)
            print("records from join" + str(records))
            
            for record in records:
                output_line = []
                for clause in parser.select_clause:
                    table, field = clause.split(".")
                    output_line.append(record[field])
                print(output_line)
                
        elif self.parser["select_clause"]:
            table_datas, field_reductions = self.get_tables([["{}.".format(self.parser["table_name"])]])
            have_printed_header = False
            header = []
            output_lines = []
            print(field_reductions[0][0])
            for result in self.process_wheres(field_reductions[0][0]):
                print("item: " + str(result))
                for field in self.parser["select_clause"]:

                    if field == "*":
                        for key, value in result.items():
                            if not have_printed_header:
                                header.append(key)
                            output_lines.append(value)
                    else:
                        output_lines.append(result[field])
                have_printed_header = True
            print(header)
            print(output_lines)
            yield output_lines




    
    def process_wheres(self, field_reductions):
        where_clause = self.parser["where_clause"]
        fts_clause = self.parser["fts_clause"]
        input_data = field_reductions
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
                # table_data = list(map(lambda x: {"id": x["value"]}, filter(lambda x: x["key"].startswith(row_filter), items)))

                try:
                    table_data = []
                    for sort_key, lookup_key in sql_index.iteritems(prefix=row_filter):
                        print(sort_key)
                        print(lookup_key)
                        table_data.append({"id": data[lookup_key]}) 
                except:
                    table_data = []
                
                reductions.append([table_data, input_data])
                table_datas.append([(table_data, "id"), (input_data, "id")])
        
        for restriction, value in where_clause:
            print("Running hash join for where clause value " + str(value))
            table, field = restriction.split(".")
            and_or.append("and")
            row_filter = "S.{}.{}.{}".format(table, field, value)
            # table_data = list(map(lambda x: {"id": x["value"]}, filter(lambda x: x["key"].startswith(row_filter), items)))
            try:
                table_data = []
                for sort_key, lookup_key in sql_index.iteritems(prefix=row_filter):
                    print(sort_key)
                    print(lookup_key)
                    table_data.append({"id": data[lookup_key]}) 
            except:
                table_data = []
            reductions.append([table_data, input_data])
            table_datas.append([(table_data, "id"), (input_data, "id")])
        
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
    print("Executing sql on data node")
    data = json.loads(request.data) 
    parser = data["parser"]
    print(parser)
    def items():
        yield from SQLExecutor(parser).execute()
    return Response(json.dumps(list(items())))
