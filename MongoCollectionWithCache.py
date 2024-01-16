"""Collection class, which derives from the pymongo Collection class, and
   adds a cache to speed up queries, which are requested multiple times.
"""
from typing import Dict, Any, Optional, Mapping

from pymongo.client_session import ClientSession
from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
from pymongo.typings import _Pipeline

from QueryInfo import QueryInfo


class MongoCollectionWithCache(Collection):
    _cache: Dict[QueryInfo, Any] = {}
    _ttl: int = 60

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache = {}

    def find_one(
            self, filter: Optional[Any] = None, *args: Any, **kwargs: Any
    ):
        """Find a single document in the collection."""
        query_info = QueryInfo(query=filter, sort=kwargs.get("sort", None), skip=kwargs.get("skip", None),
                               limit=kwargs.get("limit", None), pipeline=kwargs.get("pipeline", None))
        if query_info in self._cache:
            return self._cache[query_info]
        else:
            result = super().find_one(filter, *args, **kwargs)
            self._cache[query_info] = result
            return result

    def find(self, filter: dict, *args: Any, **kwargs: Any):
        """Query the collection."""
        query_info = QueryInfo(query=filter, sort=kwargs.get("sort", None),
                               skip=kwargs.get("skip", None), limit=kwargs.get("limit", None),
                               pipeline=kwargs.get("pipeline", None))
        if query_info in self._cache:
            return iter(self._cache[query_info])
        else:
            result = super().find(filter, *args, **kwargs)
            self._cache[query_info] = list(result)
            return iter(self._cache[query_info])

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
        if pipeline_query_info in self._cache:
            return self._cache[pipeline_query_info]
        else:
            result = super().aggregate(
                pipeline, session=session, let=let, comment=comment, **kwargs
            )
            self._cache[pipeline_query_info] = list(result)
            return iter(self._cache[pipeline_query_info])
