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


        field_reductions = []
        for index, pair in enumerate(table_datas):
            pair_items = []
            for item in pair:
                name, table, join_field, size = item
                field_reduction = table_reductions(table, defaultdict(dict))
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
                        yield {**item, **match }


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
            table_datas, field_reductions = self.get_tables([["{}.{}".format(data["select_table"], data["id_field"])]])
            print(data["id_field"])
            print(data["missing_field"])
            table_datas[0].append(("fake", data["records"], data["join_field"], "smaller"))
            pprint(table_datas)
            records = []
            for index, pair in enumerate(table_datas):
                records = self.hash_join(index, table_datas)
            
            missing_fields = []
            missing_field = data["missing_field"]
            for record in records:
               missing_fields.append(record[missing_field])

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
            print(field_reductions)
            for result in self.process_wheres(field_reductions[0][0]):
                output_line = []
                for field in self.parser["select_clause"]:
                    print(result)
                    if field == "*":
                        for key, value in result.items():
                            
                            if not have_printed_header:
                                header.append(key)
                            output_line.append(value)
                    else:
                        table, field_name = field.split(".")
                        output_line.append(result[field_name])
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
    print("Executing sql on data node")
    data = json.loads(request.data) 
    parser = data["parser"]
    def items():
        yield from SQLExecutor(parser).execute()
    return Response(json.dumps(list(items())))

@app.route("/networkjoin", methods=["POST"])
def networkjoin():
    print("Executing network join on data node")
    data = json.loads(request.data) 
    def items():
        yield from SQLExecutor(data["parser"]).networkjoin(data)
    return Response(json.dumps(list(items())))
