import pickle as pkl
import gzip
import os
import osmapi
import time


def write_pickle_zip(file_name:str, new_values:list):
    """ If key is none, creates a new pickle file. Key specifies the sub list."""
    #
    # data = {
    #     "nodes": [],
    #     "ways": [],
    #     "users": [],
    #     "chapters": [],
    #     "regions": []
    # }

    if len(new_values) > 0:
        data = []
        if os.path.exists(file_name):
            with gzip.open(file_name, 'rb+') as fd:
                data = pkl.load(fd)
                assert isinstance(data, list)

        with gzip.open(file_name, 'wb+') as fd:
            for ele in new_values:
                data.append(ele)
            # write updated data back to pickle-zip
            pkl.dump(data, fd)



def download_changesets(changeset_ids:list, last_id_dl = None):
    """

    :param changeset_ids:
    :param last_id_dl: If previous attempt to download failed due to request limiting etc.
                        Provide the last queried id to resume where you left off
    :return: None... Writes to a compressed pickle file.
    """
    num_cs = 46486
    N = 1000
    num_dl = 0

    if last_id_dl != None:
        changeset_ids = [cs_id[0] for cs_id in changeset_ids if cs_id[0] > last_id_dl]


    batches = (changeset_ids[i:i + N] for i in range(0, len(changeset_ids), N))
    api = osmapi.OsmApi()
    for b in batches:
        cs_data = []
        for cs_id in b:
            cs_data.append(api.ChangesetGet(cs_id))

        print("{0:.2f}% Downloaded. Last ID: {1}".format(num_dl/num_cs, cs_data[-1]))
        write_pickle_zip("./data/changeset_data.pklz", cs_data)
        num_dl += N
        time.sleep(30)


with gzip.open("./data/changeset_id_list.pklz", 'rb+') as f:
    ids = pkl.load(f)

download_changesets(ids)





