""" Benchmark test for find call of MongoCollectionWithCache. """

import time

import numpy as np
from pymongo import MongoClient

from benchmark_tests.test_database_setup import (
    setup_database,
    BENCHMARK_DB,
    BENCHMARK_COLL,
)
from cache_backend.CacheBackend import CacheBackend
from pymongo_wrappers.CacheFunctions import CacheFunctions
from pymongo_wrappers.MongoClientWithCache import MongoClientWithCache


def simple_find_one_test(
    address: str,
    port: int,
    nr_entries: int = 1000000,
    nr_fields: int = 20,
    nr_runs: int = 10,
    cache_backend: CacheBackend = CacheBackend.IN_MEMORY,
):
    print(f"Setting up database with {nr_entries} entries and {nr_fields} fields.")
    setup_database(address, port, nr_entries, nr_fields)
    # Connect to the database
    client = MongoClient(address, port)
    cache_client = MongoClientWithCache(
        address,
        port,
        cache_backend=cache_backend,
        functions_to_cache=[CacheFunctions.FIND_ONE],
    )

    db_cache = cache_client[BENCHMARK_DB]
    collection_cache = db_cache[BENCHMARK_COLL]

    db_no_cache = client[BENCHMARK_DB]
    coll_no_cache = db_no_cache[BENCHMARK_COLL]

    reference_times = []
    cache_times_after_caching = []

    # Warm up the cache
    start = time.time_ns()
    entry = collection_cache.find_one({"test1": 1})
    end = time.time_ns()
    initial_cache_time = (end - start) / 1e6

    for i in range(nr_runs):
        start = time.time_ns()
        entry = coll_no_cache.find_one({"test1": 1})
        end = time.time_ns()
        reference_times.append((end - start) / 1e6)

        start = time.time_ns()
        entry = collection_cache.find_one({"test1": 1})
        end = time.time_ns()
        cache_times_after_caching.append((end - start) / 1e6)

        print(f"Run {i} done.", end="\r")

    print(
        f"Mean of reference times: {np.mean(reference_times)} ms after {nr_runs} runs"
    )
    print(f"Std of reference times: {np.std(reference_times)} ms after {nr_runs} runs")
    print(
        f"Mean of cache times: {np.mean(cache_times_after_caching)} ms after {nr_runs} runs"
    )
    print(
        f"Std of cache times: {np.std(cache_times_after_caching)} ms after {nr_runs} runs"
    )

    print(f"Initial cache time: {initial_cache_time}")


def simple_find_test(
    address: str,
    port: int,
    nr_entries: int = 1000000,
    nr_fields: int = 20,
    nr_runs: int = 10,
    nr_limit: int = 100000,
    cache_backend: CacheBackend = CacheBackend.IN_MEMORY,
):
    print(f"Setting up database with {nr_entries} entries and {nr_fields} fields.")
    setup_database(address, port, nr_entries, nr_fields)
    # Connect to the database
    client = MongoClient(address, port)
    cache_client = MongoClientWithCache(
        address,
        port,
        cache_backend=cache_backend,
        functions_to_cache=[CacheFunctions.FIND],
        cache_cleanup_cycle_time=15000,
    )

    db_cache = cache_client[BENCHMARK_DB]
    collection_cache = db_cache[BENCHMARK_COLL]

    db_no_cache = client[BENCHMARK_DB]
    coll_no_cache = db_no_cache[BENCHMARK_COLL]

    reference_times = []
    cache_times_after_caching = []

    # Warm up the cache
    start = time.time_ns()
    entry = list(collection_cache.find({}, limit=nr_limit))
    end = time.time_ns()
    initial_cache_time = (end - start) / 1e6

    for i in range(nr_runs):
        start = time.time_ns()
        entries = list(coll_no_cache.find(limit=nr_limit))
        end = time.time_ns()
        reference_times.append((end - start) / 1e6)

        start = time.time_ns()
        entries = list(collection_cache.find({}, limit=nr_limit))
        end = time.time_ns()
        cache_times_after_caching.append((end - start) / 1e6)

        print(f"Run {i} done.", end="\r")

    print(
        f"Mean of reference times: {np.mean(reference_times)} ms after {nr_runs} runs"
    )
    print(f"Std of reference times: {np.std(reference_times)} ms after {nr_runs} runs")
    print(
        f"Mean of cache times: {np.mean(cache_times_after_caching)} ms after {nr_runs} runs"
    )
    print(
        f"Std of cache times: {np.std(cache_times_after_caching)} ms after {nr_runs} runs"
    )

    print(f"Initial cache time: {initial_cache_time}")


def main():
    cache_backend_to_use = CacheBackend.IN_MEMORY

    simple_find_one_test(
        "localhost",
        27017,
        nr_runs=1000,
        nr_fields=25,
        nr_entries=100000,
        cache_backend=cache_backend_to_use,
    )
    print()
    simple_find_test(
        "localhost",
        27017,
        nr_runs=1000,
        nr_fields=25,
        nr_entries=100000,
        nr_limit=1000,
        cache_backend=cache_backend_to_use,
    )


if __name__ == "__main__":
    main()
