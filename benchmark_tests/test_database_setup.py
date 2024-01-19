""" Functions for setting up the database for benchmark tests. """
from pymongo import MongoClient

BENCHMARK_COLL = "benchmark_coll"

BENCHMARK_DB = "benchmark_db"


def setup_database(
    address: str, port: int, nr_entries: int = 1000000, nr_fields: int = 1
):
    """Sets up the database for benchmark tests."""

    client = MongoClient(address, port)
    db = client[BENCHMARK_DB]
    coll = db[BENCHMARK_COLL]
    coll.drop()

    coll.insert_many(
        [{f"test{j}": j for j in range(nr_fields)} for i in range(nr_entries)]
    )

    client.close()
