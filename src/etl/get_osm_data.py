import pickle as pkl
import gzip
import os
import osmapi
import time



def write_pickle_zip(file_name:str, new_values:dict = None):
    """ If key is none, creates a new pickle file. Key specifies the sub list."""

    data = {
        "nodes": [],
        "ways": [],
        "users": [],
        "chapters": [],
        "regions": []
    }

    if os.path.exists(file_name):
        with gzip.open(file_name, 'rb+') as fd:
            data = pkl.load(fd)
            assert isinstance(data, dict)

    with gzip.open(file_name, 'wb+') as fd:
        if new_values != None:
            for key in new_values:
                assert isinstance(new_values[key], list)

            for key in new_values:
                try:
                    sublist = data[key]
                    sublist.extend(new_values[key])
                    data[key] = sublist
                except KeyError as e:
                    print(e)
            # write updated data back to pickle-zip

        pkl.dump(data, fd)


def write_chapters():
    global f
    with open("chapter_data.pkl", "rb+") as f:
        chapters = pkl.load(f)
    with gzip.open("osm_data.pklz", "rb+") as f:
        data = pkl.load(f)
    data["chapters"] = chapters
    with gzip.open("osm_data.pklz", "wb+") as f:
        pkl.dump(data)



def query_nodes(node_ids:list, batch_size = 500):
    batches = (node_ids[i:i+batch_size] for i in range(0, len(node_ids), batch_size) )

    api = osmapi.OsmApi()

    for b in batches:
        nodes = list(api.NodesGet(b).values())
        users = set()
        for n in nodes:
            try:
                u = n["user"], n["uid"]
                users.add(u)
            except:
                print("failed to extract user info from: ", n)

        write_pickle_zip("osm_data.pklz", {"nodes" : nodes, "users": list(users)})



def query_ways(way_ids:list, batch_size=500):
    batches = (way_ids[i:i + batch_size] for i in range(0, len(way_ids), batch_size))

    api = osmapi.OsmApi()

    node_ids = set()
    users = set()
    for b in batches:

        ways = list(api.WaysGet(b).values())
        for w in ways:
            for n in w["nd"]:
                node_ids.add(n)
            u = w["user"], w["uid"]
            users.add(u)

        write_pickle_zip("osm_data.pklz", {"ways" : ways, "users" : list(users)})
        with open("query_data.pkl", "rb") as fd:
            data = pkl.load(fd)

        cur = data['node_ids']
        nids = set(cur).union(node_ids)
        data["node_ids"] = list(nids)

        with open("query_data.pkl", "wb+") as fd2:
            pkl.dump(data, fd2)


# with open("query_data.pkl", "rb+") as f1:
#     query_data = pkl.load(f1)
#
# start = time.time()
# query_ways(query_data["way_ids"])
# print("Ways downloaded in {} min(s)", (time.time() - start) / 60)

with open("query_data.pkl", "rb+") as f2:
    query_data = pkl.load(f2)

start = time.time()
query_nodes(query_data["node_ids"])
print("Nodes downloaded in {} min(s)", (time.time() - start) / 60)


write_chapters()

with gzip.open("osm_data.pklz") as f:
    final_result = pkl.load(f)
    for k in final_result:
        print(k, "--" , len(final_result[k]))





