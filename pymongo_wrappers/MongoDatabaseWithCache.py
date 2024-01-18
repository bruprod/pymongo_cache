"""Mongo database class with cache."""
from pymongo.database import Database

from cache_backend.CacheBackend import CacheBackend
from pymongo_wrappers.CacheFunctions import DEFAULT_CACHE_FUNCTIONS
from pymongo_wrappers.MongoCollectionWithCache import MongoCollectionWithCache


class MongoDatabaseWithCache(Database):
    """
    Mongo client class with cache.
    """
    _cache_backend: CacheBackend = CacheBackend.IN_MEMORY
    _functions_to_cache = None

    def __init__(self, *args, cache_backend: CacheBackend = CacheBackend.IN_MEMORY, functions_to_cache=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache_backend = cache_backend
        if functions_to_cache is None:
            self._functions_to_cache = DEFAULT_CACHE_FUNCTIONS
        else:
            self._functions_to_cache = functions_to_cache

    def __getitem__(self, item):
        return MongoCollectionWithCache(self, item, cache_backend=self._cache_backend,
                                        functions_to_cache=self._functions_to_cache)
