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


class MongoCollectionWithCache(Collection):
    _cache_backend: CacheBackendBase = None
    __regular_collection = None

    def __init__(self, *args, cache_backend: CacheBackend = CacheBackend.IN_MEMORY, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache_backend = CacheBackendFactory.get_cache_backend(cache_backend)(self)
        self.__regular_collection = Collection(self.database, self.name)

    def find_one(self, filter: Optional[Any] = None, *args: Any, **kwargs: Any):
        """Find a single document in the collection."""
        query_info = QueryInfo(query=filter, sort=kwargs.get("sort", None), skip=kwargs.get("skip", None),
                               limit=kwargs.get("limit", None), pipeline=kwargs.get("pipeline", None))

        if self._cache_backend.has_item(query_info):
            return self._cache_backend.get(query_info)
        else:
            result = self.__regular_collection.find_one(filter, *args, **kwargs)
            self._cache_backend.set(query_info, result)
            return result

    def find(self, filter: dict, *args: Any, **kwargs: Any):
        """Query the collection."""
        query_info = QueryInfo(query=filter, sort=kwargs.get("sort", None),
                               skip=kwargs.get("skip", None), limit=kwargs.get("limit", None),
                               pipeline=kwargs.get("pipeline", None))

        if self._cache_backend.has_item(query_info):
            return iter(self._cache_backend.get(query_info))
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
        pipeline_query_info = QueryInfo(pipeline=pipeline)
        if self._cache_backend.has_item(pipeline_query_info):
            return iter(self._cache_backend.get(pipeline_query_info))
        else:
            result = self.__regular_collection.aggregate(
                pipeline, session=session, let=let, comment=comment, **kwargs
            )
            self._cache_backend.set(pipeline_query_info, list(result))
            return iter(self._cache_backend.get(pipeline_query_info))
