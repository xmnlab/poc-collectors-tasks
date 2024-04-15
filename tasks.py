# import multiprocessing as mp
from collections import defaultdict
from pathlib import Path


DATA_DIR = Path("data")


def get_data(file_name: str):
    data = []
    with open(DATA_DIR / file_name, "r") as f:
        data = f.readlines()[1:]

    for row in data:
        yield row.replace("\n", "").split(",")


def map_reduce_sorted():
    buffer = []
    reduce = {}
    previous = None

    for data_raw in get_data("results_sorted.txt"):
        data = {
            "group_id": int(data_raw[0]),
            "challenge_id": int(data_raw[1]),
            "points": int(data_raw[2]),
        }

        if previous is None:
            previous = data["group_id"]
            buffer = []
        elif previous != data["group_id"]:
            reduce[previous] = sum(buffer)
            previous = data["group_id"]
            buffer = []

        buffer.append(data["points"])

    reduce[previous] = sum(buffer)

    print(reduce)


def map_reduce_unsorted():
    buffer = defaultdict(list)
    reduce = {}
    previous = None

    for data_raw in get_data("results_unsorted.txt"):
        data = {
            "group_id": int(data_raw[0]),
            "challenge_id": int(data_raw[1]),
            "points": int(data_raw[2]),
            "length_group": int(data_raw[3]),
        }

        gid = data["group_id"]

        buffer[gid].append(data["points"])

        if len(buffer[gid]) == data["length_group"]:
            reduce[gid] = sum(buffer[gid])
            del buffer[gid]

    print(reduce)


print("map reduce sorted")
map_reduce_sorted()

print("map reduce unsorted")
map_reduce_unsorted()
