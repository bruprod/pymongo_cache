# pymongo_cache

A library containing as wrapper for collection, database and mongoclient implementation of pymongo.
The basic implementation stores the data for each MongoCollectionWithCache in an in-memory cache, which
is a member of the class.

## How long does the cache store the data?

The cache stores the data as long as the MongoClientWithCache instance is alive. If the instance is destroyed,
the cache is destroyed as well. The MongoClientWithCache also holds references to the MongoDatabaseWithCache
to keep them alive.

## Supported cache backends

- InMemoryCacheBackend: Stores the data in an in-memory cache
- MongoDbCacheBackend: Stores the data in an own database for caching inside the MongoDB instance

## Features

- Caching Strategies
    - LRU: entry which was least recently used is removed
    - LFU: entry which was least frequently used is removed
    - EXECUTION_TIME: entry which was executed the longest time ago is removed

- Currently Supported Functions for caching
    - find (returns no cursor, but an iterator over the results)
    - find_one (full support for all parameters)
    - aggregate (returns no CommandCursor, but an iterator over the results)
    - all functions which are not listed above are not cached and are directly forwarded to the pymongo collection class

- Parameters for the MongoClientWithCache
    - cache_backend: The cache backend to use (default: InMemoryCacheBackend)
    - cache_strategy: The cache strategy to use (default: LRU)
    - functions_to_cache: The functions which should be cached (default: ["find", "find_one", "aggregate"])
    - cache_cleanup_cycle_time: The interval in seconds in which the cleanup function is called (default: 5.)
## Requirements

- pymongo must be installed

## Examples

In the following examples, the database "Data" and the collection "Collection" are used.

### Simple example

```python
import time

from pymongo_wrappers.MongoClientWithCache import MongoClientWithCache

client = MongoClientWithCache()
db = client["Data"]

collection = db["Collection"]
collection.insert_many([{"ticker_name": "AAPL", "price": 100}, {"ticker_name": "AAPL", "price": 200}])

start = time.time_ns()
entries = list(collection.find({"ticker_name": "AAPL"}))
end = time.time_ns()
print(f"Time taken before caching: {(end - start) / 1e6} milliseconds. Entries: {len(entries)}")

start = time.time_ns()
entries = list(collection.find({"ticker_name": "AAPL"}))
end = time.time_ns()
print(f"Time taken after caching: {(end - start) / 1e6} milliseconds. Entries: {len(entries)}")
```

### Setting the cache backend to use

```python

from cache_backend.CacheBackend import CacheBackend
from pymongo_wrappers.MongoClientWithCache import MongoClientWithCache

client = MongoClientWithCache(cache_backend=CacheBackend.IN_MEMORY)

```

## Outlook

- Handler for change streams of the collections, which invalidates the cache
- Adding a cache-backend for sqlite
- Implementing a more sufficient cleanup strategy for the cache
- Adding Cursor support for find and aggregate
- TTL to the cache entries
- Supporting a max_item_size for the cache entries
- Being able to set the cache size on MongoClientWithCache

