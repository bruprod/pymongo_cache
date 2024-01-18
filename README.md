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
import time

from pymongo_wrappers.MongoClientWithCache import MongoClientWithCache
from cache_backend.CacheBackend import CacheBackend

client = MongoClientWithCache(cache_backend=CacheBackend.IN_MEMORY)

```


## Outlook
- TTL to the cache
- LRU cache
- Handler for change streams of the collections, which invalidates the cache
- Adding a cache-backend for sqlite
