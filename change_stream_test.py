""" Test script for change stream handling. """
from pymongo import MongoClient

from benchmark_tests.test_database_setup import BENCHMARK_DB, BENCHMARK_COLL


def main():
    """Main function."""
    mongo_client = MongoClient("localhost", 27017)

    db = mongo_client[BENCHMARK_DB]
    coll = db[BENCHMARK_COLL]

    with coll.watch() as stream:
        while stream.alive:
            change = stream.try_next()
            if change is not None:
                print(change)


if __name__ == "__main__":
    main()
