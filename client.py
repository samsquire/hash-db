import types
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
from operator import itemgetter
import itertools
import numpy as np
from pprint import pprint
import copy

parser = ArgumentParser()
parser.add_argument("--server")
parser.add_argument("--port")
args = parser.parse_args(os.environ["HASHDB_ARGS"].split(" "))

self_server = "localhost" + ":" + args.port

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
for lookup_key, value in bootstrapped_keys.items():
    data[lookup_key] = value
    indexed[lookup_key] = True
    partition_key, sort_key = lookup_key.split(":")
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

app = Flask(__name__)


@app.route("/get/<lookup_key>", methods=["POST"])
def get(lookup_key):
    return make_response(str(data[lookup_key]))

@app.route("/set/<partition_key>/<sort_key>", methods=["POST"])
def set_value(partition_key, sort_key):

     
    lookup_key = partition_key + ":" + sort_key
    print("{} Saving {} to {}".format(self_server, request.data, lookup_key))
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
    try:
        partition_key, sort_key = lookup_key.split(":")
        del sql_index[sort_key] 
        del data[lookup_key]
    except KeyError:
        pass
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


def deindex(partition_key, table, identifier, field_name, value):

    items_to_be_deleted = [ 
            ]

    new_key = "R.{}.{}.{}".format(table, identifier, field_name)
    items_to_be_deleted.append(new_key)
    new_key = "S.{}.{}.{}.{}".format(table, field_name, value, identifier)
    items_to_be_deleted.append(new_key)
    new_key = "C.{}.{}.{}".format(table, field_name, identifier)
    items_to_be_deleted.append(new_key)

    if isinstance(value, str):
        tokens = value.split(" ")
        if tokens:
            for token in tokens:
                new_key = "FTS.{}.{}.{}.{}".format(table, field_name, token, identifier)
                items_to_be_deleted.append(new_key)

    for item in items_to_be_deleted:
        sort_key = item
        lookup_key = partition_key + ":" + sort_key
        try:
            del indexed[lookup_key]
            del sort_index[lookup_key]
            del sort_index[sort_key][partition_key]
            del sort_index[sort_key]
            # if partition_key not in between_index:
            #     between_index[partition_key] = Tree("", None, None)
            between_index[partition_key].delete(sort_key)
            # if partition_key not in partition_trees:
            #    partition_tree = both_between_index.insert(partition_key, Tree("", None, None))
            #    partition_trees[partition_key] = partition_tree
            # between_index[partition_key].insert(sort_key, partition_key, partition_key + ":" + sort_key)
            # partition_trees[partition_key].partition_tree.insert(sort_key, partition_key, partition_key + ":" + sort_key)

            del sql_index[sort_key]
            del data[lookup_key]
        except KeyError:
            pass

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
                pair_data.append((table, table_data, field, "smaller"))
            table_datas.append(pair_data)


        def table_reductions(table, metadata):
            try:
                for record in table:
                    yield from reduce_table(metadata, record)

                if metadata["current_record"] != {}:
                    print(metadata["current_record"])
                    yield metadata["current_record"]
            except:
                if metadata["current_record"] != {}:
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
                print(table_metadata["current_record"])
                yield table_metadata["current_record"]
                # reset
                table_metadata["current_record"] = {}
                table_metadata["current_record"]["internal_id"] = identifier
                table_metadata["current_record"][field_name] = data[lookup_key]
            elif last_id == identifier:
                table_metadata["current_record"][field_name] = data[lookup_key]


        def table_enricher(table, collection):
            for row in collection:
                if "id" in row:
                    row["{}_{}".format(table, "id")] = row["id"]
                yield row

        field_reductions = []
        for index, pair in enumerate(table_datas):
            pair_items = []
            for item in pair:
                name, table, join_field, size = item
                field_reduction = list(table_enricher(name, table_reductions(table, defaultdict(dict))))
                pair_items.append(field_reduction)
            field_reductions.append(pair_items)
        
        for table_data, field_reduction in zip(table_datas, field_reductions):
            for index, old_row in enumerate(table_data):
                table_data[index] = (old_row[0], field_reduction[index], old_row[2], old_row[3])

        return table_datas, field_reductions
    
    def hash_join_simple(self, join_table, scan, test):
        ids_for_key = defaultdict(list)

        field = "{}_{}".format(join_table, "id")
        for item in itertools.chain(scan):
            if field not in item:
                continue
            left_field = item[field]
            if left_field in ids_for_key:
                ids_for_key[left_field].append(item)
            else:
                ids_for_key[left_field] = [item]

        try:
            for item in itertools.chain(test):
                
                if field in item and item[field] in ids_for_key:
                    item_value = item[field]
                    print("Join merge Found match: {} in ids_for_key".format(item_value))
                    for match in ids_for_key[item[field]]:
                        print("Yielding match result!")
                        yield {**match,**item }


        except KeyError:
            pass

    def hash_join(self, index, table_datas):
        ids_for_key = defaultdict(list)
        lhs = 0
        scan = None
        for innerindex, entry in enumerate(table_datas[index]):
            name, collection, fieldname, size = entry
            if size == "smaller":
                lhs = innerindex
                scan = collection
                break 

        for item in itertools.chain(scan):
            field = table_datas[index][lhs][2]
            print("Saving {} in probe for {}".format(item, field))
            if field not in item:
                continue
            left_field = item[field]
            if left_field in ids_for_key:
                print("Already a value for {}".format(left_field))
                ids_for_key[left_field].append(item)
            else:
                ids_for_key[left_field] = [item]
        
        test = None
        rhs = 1
        for rhsindex, entry in enumerate(table_datas[index]):
            name, collection, fieldname, size = entry
            if collection is not scan:
                rhs = rhsindex
                test = collection
                break

        try:
            for item in itertools.chain(test):
                
                if table_datas[index][rhs][2] in item and item[table_datas[index][rhs][2]] in ids_for_key:
                    item_value = item[table_datas[index][rhs][2]]
                    print("Found match: {} in ids_for_key".format(item_value))
                    for match in ids_for_key[item[table_datas[index][rhs][2]]]:
                        print("Yielding match result!")
                        yield {**match, **item}
                        
        except KeyError:
            pass

    def get_table_size(self, table_name):
        if table_name not in table_counts:
            table_counts[table_name] = 0
        return table_counts[table_name]
         

    def networkjoin(self, data):
            missing_fields = []
            for join_spec in data["join_specs"]:
                table_datas, field_reductions = self.get_tables([["{}.{}".format(join_spec["select_table"], join_spec["id_field"])]])
                print(join_spec["id_field"])
                print(join_spec["missing_field"])
                table_datas[0].append(("fake", data["records"], join_spec["join_field"], "smaller"))
                pprint(table_datas)
                records = []
                for index, pair in enumerate(table_datas):
                    records = self.hash_join(index, table_datas)
                
                missing_field = join_spec["missing_field"]
                for record in records:
                   if missing_field in record:
                       missing_fields.append([missing_field, record["missing_index"], record[missing_field]])

            yield from missing_fields
              
    def mark_join_table(self, table_datas, field_reductions, join_table):

        def enrich_table(collection):
            for item in collection:
                if "id" in item:
                    item["{}_{}".format(join_table, "id")] = item["id"]
                    yield item

        new_table_datas = []
        new_field_reductions = []
        for pair in table_datas: 
            new_pair = []
            for entry in pair:
                name, table, field, size = entry
                if name == join_table:
                    table = enrich_table(table)
                new_pair.append((name, table, field, size)) 
                new_field_reductions.append(table)
            new_table_datas.append(new_pair)
        return new_table_datas, field_reductions
    
    def rewrite_joins(self, table_datas):
        new_table_datas = [] 
        new_table_datas.append(table_datas[0]) 
        
        for table_data in table_datas[1:]:
            table_name, collection, field, size = table_data[0]
            new_table_datas.append([(table_name, "previous", field, size), table_data[1]])
            new_table_datas.append([(table_name, "previous", field, size), (table_name, collection, field, size)])
        return new_table_datas[:-1]

    def execute(self):
        if self.parser["updates"]:
            table_datas, field_reductions = self.get_tables([["{}.".format(self.parser["update_table"])]])
            for result in self.process_wheres(field_reductions[0][0]):
                for update in self.parser["updates"]:
                    updated_to, new_value = update
                    updated_field = updated_to.split(".")[1]
                    partition_key = "{}.{}".format(self.parser["update_table"], result["id"])

                    deindex(partition_key, self.parser["update_table"], result["id"], updated_field, result[updated_field])

                    # now update the data
                    items = []
                    insert_table = self.parser["update_table"]
                    field = updated_field
                    
                    if isinstance(new_value, str): 
                        tokens = new_value.replace(",", "").split(" ")
                        for token in tokens:
                            new_key = "FTS.{}.{}.{}.{}".format(insert_table, field, token, result["id"])
                            items.append({
                                "key": new_key,
                                "value": result["id"]
                            })
                    
                    
                    new_key = "R.{}.{}.{}".format(insert_table, result["id"], field)
                    items.append({
                        "key": new_key,
                        "value": new_value
                    })
                    new_key = "S.{}.{}.{}.{}".format(insert_table, field, new_value, result["id"])
                    items.append({
                        "key": new_key,
                        "value": result["id"]
                    })
                    new_key = "C.{}.{}.{}".format(insert_table, field, result["id"])
                    items.append({
                        "key": new_key,
                        "value": new_value
                    })

                    items.sort(key=itemgetter('key'))


                    for item in items:
                        sort_key = item["key"]
                        lookup_key = partition_key + ":" + sort_key
                        data[lookup_key] = item["value"]

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

                
            
        elif self.parser["fts_clause"]:
            # full text search
            table_datas, field_reductions = self.get_tables([["{}.".format(self.parser["table_name"])]])
            have_printed_header = False
            header = []
            output_lines = []
            outputs = []
            for result in self.process_wheres(field_reductions[0][0]):
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
            yield from output_lines
            
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
            table_datas, field_reductions = self.get_tables(self.parser["join_clause"])

            table_datas, field_reductions = self.mark_join_table(table_datas, field_reductions, self.parser["table_name"])
            table_datas = self.rewrite_joins(table_datas)
            pprint(table_datas)

            previous = list(self.hash_join(0, table_datas))
            pprint(table_datas)
            print("First join")
            pprint(previous) 
            for index, pair in enumerate(table_datas[1:]):
                entries = table_datas[index + 1] 
                table_name, collection, field, size = entries[0]
                if collection == "previous":
                    table_datas[index + 1][0] = (table_name, previous, field, size)  

                previous = list(self.hash_join(index + 1, table_datas))
                print("Second join")
                pprint(previous)
                

            records = self.process_wheres(previous)
            print("records from join" + str(records))
            print(len(records))
            missing_fields = set() 
            output_lines = []
            pprint(records)
            for record in records:
                # output_line = []
                output_lines.append(record)
                for clause in self.parser["select_clause"]:
                    table, field = clause.split(".")
                    if field not in record:
                        missing_fields.add(field)
                # output_lines.append(output_line)
            print(len(output_lines))
            print(len(records))
            yield {"outputs": output_lines, "missing_fields": list(missing_fields)}
                
        elif self.parser["select_clause"]:
            table_datas, field_reductions = self.get_tables([["{}.".format(self.parser["table_name"])]])
            have_printed_header = False
            header = []
            output_lines = []
            for result in self.process_wheres(field_reductions[0][0]):
                skip = False
                output_line = []
                for field in self.parser["select_clause"]:
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

            yield from output_lines
            print(header)
            print(output_lines)



    
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
                table_datas.append([("name", table_data, "id", "smaller"), (input_data, "id", "bigger")])
        
        for restriction, value in where_clause:
            print("Running hash join for where clause value " + str(value))
            table, field = restriction.split(".")
            and_or.append("and")
            row_filter = "S.{}.{}.{}".format(table, field, value)
            # table_data = list(map(lambda x: {"id": x["value"]}, filter(lambda x: x["key"].startswith(row_filter), items)))
            def tabledata():
                try:
                    for sort_key, lookup_key in sql_index.iteritems(prefix=row_filter):
                        print(sort_key)
                        print(lookup_key)
                        yield {"id": data[lookup_key]}
                except KeyError:
                    pass
            table_data = tabledata() 
            reductions.append([table_data, input_data])
            table_datas.append([("name", table_data, "id", "smaller"), ("name", input_data, "id", "bigger")])
        
        
        for index, pair in enumerate(table_datas):
            records = list(self.hash_join(index, table_datas))
                
        return records

@app.route("/sql", methods=["POST"])
def sql():
    print("Executing sql on data node {}".format(self_server))
    data = json.loads(request.data) 
    parser = data["parser"]
    def items():
        yield from SQLExecutor(parser).execute()
    return Response(json.dumps(list(items())))

@app.route("/networkjoin", methods=["POST"])
def networkjoin():
    print("Executing network join on data node {}".format(self_server))
    data = json.loads(request.data) 
    def items():
        yield from SQLExecutor(data["parser"]).networkjoin(data)
    return Response(json.dumps(list(items())))

class Graph:
    
    def __init__(self):
        self.nodes = []
        self.data = {}
        self.index = {}
        self.relationships = {}
        self.directions = defaultdict(dict)
        self.attribute_index = {}
        self.size = 0
        
    def add_relationship(self, name):
        self.data[name] = np.empty((self.size, self.size))
        self.data[name].fill(0)

    def index_attributes(self, position, attributes):
        for key, value in attributes.items():
            lookup_string = "{}={}".format(key, value)
            if lookup_string in self.attribute_index:
                self.attribute_index[lookup_string].append(position)
            else:
                self.attribute_index[lookup_string] = [position]
        
    def ensure_relationship(self, name):
        if name not in self.data:    
            self.add_relationship(name)
        

    def add_node(self, name, attributes):
        position = len(self.nodes)
        attributes["name"] = name
        data = {"position": position, "name": name, "attributes": attributes}
        self.index[name] = data
        
        self.index_attributes(position, attributes)
        
        self.nodes.append(data)
        self.size = position + 1
        for relationship, relationship_data in self.data.items():
            print("Resizing data {} {}".format(self.size, self.size))
            pprint("Before resize")
            pprint(self.data[relationship].shape)
           
            self.data[relationship] = np.hstack([relationship_data, np.zeros([self.data[relationship].shape[0], 1 ])])
            self.data[relationship] = np.vstack([self.data[relationship], np.zeros([1, self.data[relationship].shape[1]])])
            pprint("After resize")
            pprint(self.data[relationship].shape)
    
    def add_edge(self, _from, to, relationship):
        from_node = self.index[_from]
        to_node = self.index[to]
        print("{} ({}) -> {} ({})".format(
            from_node["name"], from_node["position"], to_node["name"], to_node["position"]))
        
        self.data[relationship][from_node["position"]][to_node["position"]] = 1
        self.data[relationship][to_node["position"]][from_node["position"]] = 1
        
        direction_index = "{}_{}".format(from_node["position"], to_node["position"])
        print(direction_index)                            
        self.directions[relationship][direction_index] = True
        if to == "Tasya":
            pprint(self.data[relationship])
        # self.relationships["{}_{}".format(from_node["position"], to_node["position"])] = attributes
        
        
    
    def edges_from(self, relationship, node):
        multiply = np.empty((self.data[relationship].shape[0], self.data[relationship].shape[1]))
        print("Multiply matrix")
        multiply.fill(0)
        for item in range(0, self.data[relationship].shape[1]):
            multiply[self.index[node]["position"], item] = 1
            # multiply[item, self.index[node]["position"]] = 1
        pprint(multiply)
        print("Result matrix")
        edges_from = np.matmul(self.data[relationship], multiply)
        pprint(edges_from)
        for item in range(0, edges_from.shape[1]):
            print("{}? {}".format(self.nodes[item]["name"], edges_from[item, item]))
        print("Edges from them them onwards")
        from_them = np.matmul(self.data[relationship], edges_from)
        print("From them")
        pprint(from_them)
        for item in range(0, from_them.shape[1]):
            print("{}? {}".format(self.nodes[item]["name"], from_them[item, item]))
    
    def find_nodes_from_attributes(self, attributes):
        matches = set()
        for key, value in attributes.items():
            lookup_string = "{}={}".format(key, value)
            if lookup_string in self.attribute_index:
                for node in self.attribute_index[lookup_string]:
                    
                    matches.add(node)
        for item in matches:
            yield self.nodes[item]
    
    def find_nodes_by_label(self, label):
        return self.find_nodes_from_attributes({"label": label})
    
    def create_matrix(self, adjacency_matrix, nodes):
        multiply = np.empty((adjacency_matrix.shape[0], 1))
        multiply.fill(0)
        for node in nodes:
                multiply[node["position"]][0] = 1
        return multiply
    
    def query(self, parser):
        count = 0
        matching_stack = []
        matching_nodes = []
        relationships = []
        variables = {}
         
        if parser["merge"]:
            for planning_index, planning_node in enumerate(parser["graph"]):
                if planning_node["kind"] == "merge":
                    # create node if it doesn't exist
                    # first we search for it
                    matching_nodes = list(self.find_nodes_from_attributes(planning_node["attributes"]))
                    # root.add_node("Sally", {"label": "Person"})
                    
                    if not matching_nodes:
                        print("Creating node")
                        attributes = {"label": planning_node["label"]}        
                        attributes.update(planning_node["attributes"])

                        self.add_node(planning_node["attributes"]["name"], attributes)
                if planning_node["kind"] == "relationship":
                    self.ensure_relationship(planning_node["name"])


        if parser["match"]:
            for planning_index, planning_node in enumerate(parser["graph"]):
                
                
                if planning_node["kind"] == "match":
                    
                    matching_stack.append(planning_node)
                    pprint(len(matching_stack))
                    if len(matching_stack) == 1:
                        print("We in first match")
                        
                        matching_nodes = []
                        
                        if relationships:
                            # we have context to begin with
                            print("We're continuing from the beginning context")
                            
                            seen_before = planning_node["variable"] in variables
                            print(seen_before)
                            
                            
                            
                            
                        elif "attributes" in planning_node and planning_node["attributes"]:
                            # we need to find a source node based on attributes
                            matching_nodes = list(self.find_nodes_from_attributes(planning_node["attributes"]))
                            pprint(matching_nodes)
                        elif "label" in planning_node:
                            matching_nodes = self.find_nodes_by_label(planning_node["label"])
                            print("Found matching nodes")
                            
                        else:
                            print("We're starting with all nodes firstly")
                            matching_nodes = self.nodes
                        
                        if matching_nodes:
                        
                            def enrich_variable(nodes):
                                variable_name = planning_node["variable"]
                                for matching_node in nodes:
                                    copy = matching_node.copy()
                                    copy[variable_name] = True
                                    yield copy


                            if "variable" in planning_node:
                                matching_nodes = enrich_variable(matching_nodes)

                            # relationships.clear()
                            # create initial relationships

                            count = 0
                            for matching_node in matching_nodes:
                                print("Adding {}".format(matching_node["name"]))
                                matches = []
                                count = count + 1
                                relationships.append({
                                    "id": count,
                                    "matches": matches,
                                    "old_matches": [],
                                    "count": 0
                                    
                                })
                                matches.append({
                                    "to_node": matching_node,
                                    "planning_index": planning_index
                                })

                        
                        
                        if "variable" in planning_node and planning_node["variable"] not in variables:
                            variables[planning_node["variable"]] = {
                                "planning_index": planning_index,
                                "relationships": relationships,
                                "start_nodes": matching_nodes,
                                "matching_nodes": matching_nodes,
                                "usages": 0,
                                "left_hand_variable": True,
                                "filled": False
                            }
                    
                    
                    else:
                        seen_before = planning_node["variable"] in variables
                        
                        if seen_before:
                            # we have to do some merging of data
                            print("{} has been seen before - we need to merge".format(planning_node["variable"]))
                            needs_merge = True
                            old_version = variables[planning_node["variable"]]["planning_index"]
                            for relationship in relationships:
                                current_matches = relationship["matches"]
                                
                                for match in relationship["old_matches"][1:]:
                                    
                                    if match:
                                        this_index = match[0]["planning_index"]
                                        if this_index == old_version:
                                            # we need to merge matches and relationship["matches"]
                                            print("We need to merge this data")
                                            
                                            deletions = []
                                            for current_match in current_matches:
                                                from_node = current_match["to_node"]
                                                found = False
                                                for node in match:
                                                    if node["to_node"]["name"] == from_node["name"]:
                                                        found = True
                                                        break
                                                if not found:
                                                    deletions.append(current_match)
                                            for deletion in deletions:
                                                current_matches.remove(deletion)
                                                    
                                                
                                            
                                            
                                            
                            
                        
                        # we're re-matching to end nodes
                        # we don't need to keep track of the start anymore
                        starting_nodes = matching_stack.pop(0)
                        print("Length after pop {}".format(len(matching_stack)))
                        if "variable" in planning_node and planning_node["variable"] not in variables:
                            variable_name = planning_node["variable"]
                            
                            variables[variable_name] = {
                                "relationships": relationships,
                                "usages": 0,
                                "left_hand_variable": False,
                                "filled": False
                            }
                        
                        pprint("Marking as filled {}".format(planning_node["variable"]))
                        variables[planning_node["variable"]]["filled"] = True
                        
                        starting_nodes_variable = starting_nodes["variable"]
                        pprint(starting_nodes_variable)
                        if "variable" in starting_nodes and starting_nodes_variable not in variables:
                            variables[starting_nodes_variable] = {}
                        
                        
                        right_matching_nodes = []
                        print("We're merging")
                        pprint(planning_node)
                        
                        if "label" in planning_node:
                            label = planning_node["label"]
                            
                            rdeletions = []
                            for relation in relationships:
                                
                                deletions = []
                                for match in relation["matches"]:
                                    if "planning_index" not in match:
                                        match["planning_index"] = planning_index
                                    if match["to_node"]["attributes"]["label"] != label:

                                        deletions.append(match)
                                    else:
                                        print("Saving {} to {}".format(label, variable))
                                        right_matching_nodes.append(match["to_node"])
                            
                                for deletion in deletions:
                                    pass  # relationship["matches"].remove(deletion)
                                if len(relation["matches"]) == 0:
                                    # delete it
                                    # rdeletions.append(relation)
                                    pass
                            for deletion in rdeletions:
                                relationships.remove(deletion)
                                    
                        else:
                            for relationship in relationships:
                                for match in relationship["matches"]:
                                    right_matching_nodes.append(match["to_node"])
                        
                        
                        
                        if "planning_index" in variables[planning_node["variable"]]:
                            print("We need to rotate the relationships")
                            source_matches = variables[planning_node["variable"]]["planning_index"]
                            print("We need {}".format(source_matches))
                            
                            for relationship in relationships:
                                temporary_matches = None
                                for old_matches in relationship["old_matches"][1:]:
                                    if len(old_matches) > 0:
                                        this_planning_index = old_matches[0]["planning_index"]
                                        print("Found matches with planning index of {}".format(this_planning_index))
                                        if this_planning_index == source_matches:
                                            temporary_matches = relationship["matches"]
                                            relationship["matches"] = old_matches
                                            relationship["rotated"] = True
                                
                                if temporary_matches:
                                    relationship["old_matches"].append(temporary_matches)
                        
                        
                        variables[planning_node["variable"]]["planning_index"] = planning_index 
                        
                        if not seen_before:
                            for matching_node in right_matching_nodes:
                                matching_node[planning_node["variable"]] = True
                        
                        matching_stack.pop()
                            
                     
                        
                        
                
                if planning_node["kind"] == "relationship":
                    
                    active_relationships = relationships
                    left_hand_variable = False
                    variable = None
                    variable_filled = False
                    if "variable" in matching_stack[-1]:
                        pprint(matching_stack[-1])
                        variable = matching_stack[-1]["variable"]
                        left_hand_variable = variables[variable]["left_hand_variable"]                    
                        variable_filled = variables[variable]["filled"]
                        print("Found variable {}".format(variable_filled))
                        active_relationships = variables[variable]["relationships"]
                        
                    
                    relationship_name = planning_node["name"]
                    
                    print("Now searching {}".format(relationship_name))
                    
                    
                    adjacency_matrix = self.data[relationship_name]
                    new_matching_nodes = []
                    
                    deletions = []
                    
                    
                    
                    for relationship in active_relationships:
                        # if this relationship refers to left hand graph clause
                        # ie match (person:Person)-[:FRIEND_OF]->(person2:Person)
                        # person is a left hand variable because it starts the chain
                        
                        relationship["old_matches"].append(relationship["matches"])
                        
                        if left_hand_variable and relationship["count"] == 1:
                            matches = relationship["old_matches"][0]
                        elif variable and variable_filled:
                            matching_index = variables[variable]["planning_index"]
                            print("We're loading data from a past run {} for variable {}".format(matching_index, variable))
                            found = False
                            for match in relationship["old_matches"]:
                                if match:
                                    
                                    
                                    if match[0]["planning_index"] == matching_index:
                                        # we need to use this data
                                        
                                        matches = match
                                        found = True
                                        pprint(matches)
                                        break
                            if not found:
                                print("Could not find data for {}".format(relationship))
                                pprint(relationship)
                                continue
                                # matches = relationship["matches"]
                                        
                        
                        else:
                            matches = relationship["matches"]
                            
                        if relationship_name == "LIKES":
                            pprint(matches)
                        
                        
                        relationship["count"] = relationship["count"] + 1
                        
                        relationship["matches"] = []
                        
                        for match in matches:
                            node = match["to_node"]
                            multiply_matrix = self.create_matrix(adjacency_matrix, [node])
                            edges_from = np.matmul(adjacency_matrix, multiply_matrix)
                            
                            for item in range(0, edges_from.shape[0]):
                               
                                if edges_from[item][0] == 1:
                                    direction_index = "{}_{}".format(node["position"], self.nodes[item]["position"])
                                    
                                    
                                    if self.directions[relationship_name].get(direction_index, False) == True:
                                        print("{} -{}-> {}".format(node["name"], relationship_name, self.nodes[item]["name"]))    
                                        
                                        relationship["matches"].append({
                                            "relationship": relationship_name,
                                            "from_node": node,
                                            "to_node": self.nodes[item]
                                        })
                                  
                                        
                                        new_matching_nodes.append(self.nodes[item])
                                    
                        
                    
                    
                    # update the variable
                    if "variable" in matching_stack[-1]:
                        variables[matching_stack[-1]["variable"]]["relationships"] = active_relationships
                    relationships = active_relationships
        
        if parser["merge"]:
            
            for left, middle, right in zip(*[iter(parser["graph"])]*3):
                if middle["kind"] == "relationship":
                    print("Creating relationship")
                    left_node = left["attributes"]["name"]
                    right_node = right["attributes"]["name"]
                    self.add_edge(left_node, right_node, middle["name"])
        
        pprint(parser["graph"])
        pprint(relationships)
        print("Return clauses")
        pprint(parser["return_clause"])
        
        if parser["match"]:
            def getrows():
                for relation in relationships:

                    output_row = {}
                    invalid_match = False
                    for return_clause in parser["return_clause"]:
                        if return_clause not in output_row:
                                output_row[return_clause] = []
                        for matching in relation["matches"]:

                            if "rotated" not in relation:
                                if matching["from_node"].get(return_clause):
                                    output_row[return_clause].append(matching["from_node"])
                                elif matching["to_node"].get(return_clause):
                                    output_row[return_clause].append(matching["to_node"])



                        for old_matches in reversed(relation["old_matches"][1:]):
                            found = False
                            if not old_matches:
                                invalid_match = True
                            for old_match in old_matches:
                                if old_match["from_node"].get(return_clause):

                                    output_row[return_clause].append(old_match["from_node"])
                                    found = True

                                elif old_match["to_node"].get(return_clause):
                                    output_row[return_clause].append(old_match["to_node"])
                                    found = True

                            if found:
                                break
                    for return_clause in parser["return_clause"]:
                        if not output_row.get(return_clause, None):
                            invalid_match = True
                    if not invalid_match:
                        yield output_row

            outputs = getrows() 
            output_variables = [] 
            for variable in parser["return_clause"]:
                output_variables.append(variable)

            for outputrow in outputs: 
                streams = []
                for variable in parser["return_clause"]:
                    streams.append(outputrow[variable])
                
                for streamdata in zip(*streams):
                    pprint(streamdata)
                    outputrow = {}                     
                    for index, variable in enumerate(streamdata):
                        outputrow[output_variables[index]] = variable
                    yield outputrow
        

graphs = Graph()

@app.route("/cypher", methods=["POST"])
def cypher():
    print("Executing cypher on data node {}".format(self_server))
    data = json.loads(request.data) 
    parser = data["parser"]
    def items():
        yield from graphs.query(parser)
    return Response(json.dumps(list(items())))
