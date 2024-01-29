"""Mongo database class with cache."""
from typing import Optional

from pymongo.database import Database

from cache_backend.CacheBackend import CacheBackend
from pymongo_wrappers.CacheFunctions import DEFAULT_CACHE_FUNCTIONS
from pymongo_wrappers.DefaultCachingBehavior import DefaultCachingBehavior
from pymongo_wrappers.MongoCollectionWithCache import MongoCollectionWithCache


class MongoDatabaseWithCache(Database):
    """
    Mongo client class with cache.
    """

    _cache_backend: CacheBackend = CacheBackend.IN_MEMORY
    _functions_to_cache = None
    _collections_created = None
    _cache_cleanup_cycle_time = None
    _max_num_items = 1000
    _max_item_size = 1 * 10**6
    _ttl = 0
    _default_caching_behavior = None

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
        self._collections_created = {}
        self._cache_cleanup_cycle_time = cache_cleanup_cycle_time
        self._max_num_items = max_num_items
        self._max_item_size = max_item_size
        self._ttl = ttl
        self._default_caching_behavior = default_caching_behavior

    def __getitem__(self, item):
        if item in self._collections_created:
            return self._collections_created[item]

        # TODO collecton dict may not be thread safe due to being hold in a dict

        coll = MongoCollectionWithCache(
            self,
            item,
            cache_backend=self._cache_backend,
            functions_to_cache=self._functions_to_cache,
            cache_cleanup_cycle_time=self._cache_cleanup_cycle_time,
            max_num_items=self._max_num_items,
            max_item_size=self._max_item_size,
            ttl=self._ttl,
            default_caching_behavior=self._default_caching_behavior,
        )
        self._collections_created[item] = coll

        return coll
