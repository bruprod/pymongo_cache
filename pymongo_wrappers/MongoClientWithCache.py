"""Mongo client class with cache."""
from threading import Lock
from typing import Optional

from pymongo import MongoClient

from cache_backend.CacheBackend import CacheBackend
from pymongo_wrappers.CacheFunctions import DEFAULT_CACHE_FUNCTIONS
from pymongo_wrappers.DefaultCachingBehavior import DefaultCachingBehavior
from pymongo_wrappers.MongoDatabaseWithCache import MongoDatabaseWithCache

_client_dict_lock: Lock = Lock()


class MongoClientWithCache(MongoClient):
    """
    Mongo client class with cache.
    :param cache_backend: The cache backend to use for caching.
    :param functions_to_cache: The list of functions for which caching should be applied.
    :param cache_cleanup_cycle_time: The time between cache cleanups.
    :param max_num_items: The maximum number of items in the cache.
    :param max_item_size: The maximum size of an item in the cache.
    :param ttl: The time to live for an item in the cache.
    :param default_caching_behavior: The default caching behavior to use (def.
    """

    _cache_backend: CacheBackend = CacheBackend.IN_MEMORY
    _functions_to_cache = None
    _database_created = None
    _cache_cleanup_cycle_time = None
    _max_num_items = 1000
    _max_item_size = 1 * 10**6
    _ttl = 0
    _default_caching_behavior = DefaultCachingBehavior.CACHE_ALL

    def __init__(
        self,
        *args,
        cache_backend: CacheBackend = CacheBackend.IN_MEMORY,
        functions_to_cache=None,
        cache_cleanup_cycle_time: Optional[float] = None,
        max_num_items: int = 1000,
        max_item_size: int = 1 * 10**6,
        ttl: int = 0,
        default_caching_behavior: bool = DefaultCachingBehavior.CACHE_ALL,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._cache_backend = cache_backend
        if functions_to_cache is None:
            self._functions_to_cache = DEFAULT_CACHE_FUNCTIONS
        else:
            self._functions_to_cache = functions_to_cache

        self._database_created = {}
        self._cache_cleanup_cycle_time = cache_cleanup_cycle_time
        self._max_num_items = max_num_items
        self._max_item_size = max_item_size
        self._ttl = ttl
        self._default_caching_behavior = default_caching_behavior

    def __getitem__(self, name: str) -> MongoDatabaseWithCache:
        with _client_dict_lock:
            if name in self._database_created:
                return self._database_created[name]

            db = MongoDatabaseWithCache(
                self,
                name,
                cache_backend=self._cache_backend,
                functions_to_cache=self._functions_to_cache,
                cache_cleanup_cycle_time=self._cache_cleanup_cycle_time,
                max_num_items=self._max_num_items,
                max_item_size=self._max_item_size,
                ttl=self._ttl,
                default_caching_behavior=self._default_caching_behavior,
            )

            self._database_created[name] = db

            return db
