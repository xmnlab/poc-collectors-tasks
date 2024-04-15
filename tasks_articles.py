# import multiprocessing as mp
import asyncio
import time

from collections import defaultdict
from pathlib import Path


DATA_DIR = Path("data")


def get_list_articles():
    data = []
    with open(DATA_DIR / "collectors.txt", "r") as f:
        # research_id,article_id,total_articles
        data = f.readlines()

    for row in data:
        yield row.replace("\n", "").split(",")


def clean_data():
    with open(DATA_DIR / "collectors.txt", "w") as f:
        f.write("")


async def create_project(data: list, sleep_value: int):
    for row in data:
        with open(DATA_DIR / "collectors.txt", "a") as f:
            text = ",".join([str(r) for r in row])
            f.write(f"{text}\n")
        await asyncio.sleep(sleep_value)


def map_reduce_articles_researches():
    buffer = defaultdict(list)
    reduce = {}
    previous = None

    for data_raw in get_list_articles():
        data = {
            "research_id": int(data_raw[0]),
            "article_id": int(data_raw[1]),
            "total_articles": int(data_raw[2]),
        }

        gid = data["research_id"]

        # process that article
        # buffer[gid].append(data["points"])

        if len(buffer[gid]) == data["total_articles"]:
            # process that article
            # reduce[gid] = sum(buffer[gid])
            # del buffer[gid]
            ...

    print(reduce)


async def main():
    print("cleaning the data")
    clean_data()

    print("create project")
    p1 = create_project(
        [
            [1,1,3],
            [1,100,3],
            [1,3,3],
        ],
        0.1
    )
    p2 = create_project(
        [
            [2,10,2],
            [2,30,2],
        ],
        0.1
    )

    await asyncio.gather(p1, p2)

    print("map reduce articles")
    # map_reduce_articles_researches()


"""

1,1,3
2,10,2
1,100,3
3,1,1
2,30,2
1,3,3
""";


asyncio.run(main())
