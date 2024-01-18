"""Collection class, which derives from the pymongo Collection class, and
   adds a cache to speed up queries, which are requested multiple times.
"""
from typing import Any, Optional, Mapping

from pymongo.client_session import ClientSession
from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
from pymongo.typings import _Pipeline

from cache_backend.CacheBackend import CacheBackend, CacheBackendFactory
from cache_backend.CacheBackendBase import CacheBackendBase
from cache_backend.QueryInfo import QueryInfo
from pymongo_wrappers.CacheFunctions import DEFAULT_CACHE_FUNCTIONS, CacheFunctions


class MongoCollectionWithCache(Collection):
    _cache_backend: CacheBackendBase = None
    _functions_to_cache = None
    __regular_collection = None

    def __init__(self, *args, cache_backend: CacheBackend = CacheBackend.IN_MEMORY,
                 functions_to_cache=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache_backend = CacheBackendFactory.get_cache_backend(cache_backend)(self)
        self.__regular_collection = Collection(self.database, self.name)

        if functions_to_cache is None:
            self._functions_to_cache = DEFAULT_CACHE_FUNCTIONS
        else:
            self._functions_to_cache = functions_to_cache

    def find_one(self, filter: Optional[Any] = None, *args: Any, **kwargs: Any):
        """Find a single document in the collection."""
        # If the find_one function is not in the functions to cache, then just return the result of the regular find_one
        function_enum = CacheFunctions.FIND_ONE
        if function_enum not in self._functions_to_cache:
            return self.__regular_collection.find_one(filter, *args, **kwargs)

        query_info = QueryInfo(function_enum.name, query=filter, sort=kwargs.get("sort", None), skip=kwargs.get("skip", None),
                               limit=kwargs.get("limit", None), pipeline=kwargs.get("pipeline", None))

        item = self._cache_backend.get(query_info)
        if item is not None:
            return item
        else:
            result = self.__regular_collection.find_one(filter, *args, **kwargs)
            self._cache_backend.set(query_info, result)
            return result

    def find(self, filter: dict, *args: Any, **kwargs: Any):
        """Query the collection."""
        # If the find function is not in the functions to cache, then just return the result of the regular find
        function_enum = CacheFunctions.FIND
        if function_enum not in self._functions_to_cache:
            return self.__regular_collection.find(filter, *args, **kwargs)

        query_info = QueryInfo(function_enum.name, query=filter, sort=kwargs.get("sort", None),
                               skip=kwargs.get("skip", None), limit=kwargs.get("limit", None),
                               pipeline=kwargs.get("pipeline", None))

        item = self._cache_backend.get(query_info)
        if item is not None:
            return iter(item)
        else:
            result = self.__regular_collection.find(filter, *args, **kwargs)
            self._cache_backend.set(query_info, list(result))
            return iter(self._cache_backend.get(query_info))

    def aggregate(
            self,
            pipeline: _Pipeline,
            session: Optional[ClientSession] = None,
            let: Optional[Mapping[str, Any]] = None,
            comment: Optional[Any] = None,
            **kwargs: Any,
    ) -> CommandCursor:
        """Perform an aggregation using the aggregation framework on this
           collection.
        """
        # If the aggregate function is not in the functions to cache, then just return the result of the regular
        # aggregate
        function_enum = CacheFunctions.AGGREGATE
        if function_enum not in self._functions_to_cache:
            return self.__regular_collection.aggregate(
                pipeline, session=session, let=let, comment=comment, **kwargs
            )

        pipeline_query_info = QueryInfo(function_enum.name, pipeline=pipeline)
        item = self._cache_backend.get(pipeline_query_info)
        if item is not None:
            return iter(self._cache_backend.get(item))
        else:
            result = self.__regular_collection.aggregate(
                pipeline, session=session, let=let, comment=comment, **kwargs
            )
            self._cache_backend.set(pipeline_query_info, list(result))
            return iter(self._cache_backend.get(pipeline_query_info))
