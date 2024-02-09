"""Collection class, which derives from the pymongo Collection class, and
   adds a cache to speed up queries, which are requested multiple times.
"""
import time
from typing import Any, Optional, Mapping, List, Iterable, Union, Sequence, Tuple

from bson.raw_bson import RawBSONDocument
from pymongo import ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
from pymongo.operations import _IndexKeyHint, _IndexList
from pymongo.results import (
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
    DeleteResult,
)
from pymongo.typings import _DocumentType
from pymongo.typings import _Pipeline, _CollationIn

from cache_backend.CacheBackend import CacheBackend, CacheBackendFactory
from cache_backend.QueryInfo import QueryInfo
from cache_backend.base.CacheBackendBase import CacheBackendBase
from pymongo_wrappers.CacheFunctions import DEFAULT_CACHE_FUNCTIONS, CacheFunctions
from pymongo_wrappers.DefaultCachingBehavior import DefaultCachingBehavior


class MongoCollectionWithCache(Collection):
    _cache_backend: CacheBackendBase = None
    _functions_to_cache = None
    _cache_cleanup_cycle_time = None
    _max_num_items = 1000
    _max_item_size = 1 * 10**6
    _ttl = 0
    _default_caching_behavior = None

    def __init__(
        self,
        *args,
        cache_backend: CacheBackend = CacheBackend.IN_MEMORY,
        functions_to_cache: Optional[List[CacheFunctions]] = None,
        cache_cleanup_cycle_time: Optional[float] = None,
        max_num_items: int = 1000,
        max_item_size: int = 1 * 10**6,
        ttl: int = 0,
        default_caching_behavior: bool = DefaultCachingBehavior.CACHE_ALL,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._cache_backend = CacheBackendFactory.get_cache_backend(cache_backend)(
            self,
            cache_cleanup_cycle_time=cache_cleanup_cycle_time,
            max_num_items=max_num_items,
            max_item_size=max_item_size,
            ttl=ttl,
        )

        if functions_to_cache is None:
            self._functions_to_cache = DEFAULT_CACHE_FUNCTIONS
        else:
            self._functions_to_cache = functions_to_cache

        self._cache_cleanup_cycle_time = cache_cleanup_cycle_time
        self._max_num_items = max_num_items
        self._max_item_size = max_item_size
        self._ttl = ttl
        self._default_caching_behavior = default_caching_behavior

    def _check_caching_allowed(self, function_enum: CacheFunctions) -> bool:
        """Checks if caching is allowed for the given function and due to the default caching behavior."""
        if (
            function_enum in self._functions_to_cache
            and self._default_caching_behavior == DefaultCachingBehavior.CACHE_ALL
        ):
            return True
        elif self._default_caching_behavior == DefaultCachingBehavior.CACHE_ALL:
            # CACHE_ALL only caches the functions in the functions_to_cache list
            return False
        elif self._default_caching_behavior == DefaultCachingBehavior.CACHE_NONE:
            return False
        else:
            raise ValueError(
                f"Invalid default caching behavior: {self._default_caching_behavior}"
            )

    def find_one(
        self,
        filter: Optional[Any] = None,
        *args: Any,
        cache_always: bool = False,
        **kwargs: Any,
    ):
        """
        Find a single document in the collection.
        :param cache_always: If true, the query will always be cached, even
            if the function is not in the functions to cache or the default caching behavior is CACHE_NONE.
        :param filter: A query expression for MongoDb.
        """
        # If the find_one function is not in the functions to cache, then just return the result of the regular find_one
        function_enum = CacheFunctions.FIND_ONE
        if not self._check_caching_allowed(function_enum) and not cache_always:
            return Collection(self.database, self.name).find_one(
                filter, *args, **kwargs
            )

        query_info = QueryInfo(
            function_enum.name,
            query=filter,
            sort=kwargs.get("sort", None),
            skip=kwargs.get("skip", None),
            limit=kwargs.get("limit", None),
            pipeline=kwargs.get("pipeline", None),
        )

        item = self._cache_backend.get(query_info)
        if item is not None:
            return item
        else:
            start = time.time_ns()
            result = Collection(self.database, self.name).find_one(
                filter, *args, **kwargs
            )
            end = time.time_ns()
            exec_in_ms = (end - start) / 1e6
            self._cache_backend.set(query_info, result, exec_in_ms)
            return result

    def find(
        self,
        filter: Optional[dict] = None,
        *args: Any,
        cache_always: bool = False,
        **kwargs: Any,
    ):
        """
        Query the collection.
        :param cache_always: If true, the query will always be cached, even
            if the function is not in the functions to cache or the default caching behavior is CACHE_NONE.
        :param filter: A query expression for MongoDb.
        """
        # If the find function is not in the functions to cache, then just return the result of the regular find
        function_enum = CacheFunctions.FIND
        if not self._check_caching_allowed(function_enum) and not cache_always:
            return Collection(self.database, self.name).find(filter, *args, **kwargs)

        query_info = QueryInfo(
            function_enum.name,
            query=filter,
            sort=kwargs.get("sort", None),
            skip=kwargs.get("skip", None),
            limit=kwargs.get("limit", None),
            pipeline=kwargs.get("pipeline", None),
        )

        item = self._cache_backend.get(query_info)
        if item is not None:
            return iter(item)
        else:
            start = time.time_ns()
            result = Collection(self.database, self.name).find(filter, *args, **kwargs)
            end = time.time_ns()
            exec_in_ms = (end - start) / 1e6
            self._cache_backend.set(query_info, list(result), exec_in_ms)
            return iter(result)

    def aggregate(
        self,
        pipeline: _Pipeline,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        cache_always: bool = False,
        **kwargs: Any,
    ) -> CommandCursor:
        """
        Perform an aggregation using the aggregation framework on this collection.
        :param cache_always: If true, the query will always be cached, even
            if the function is not in the functions to cache or the default caching behavior is CACHE_NONE.
        """
        function_enum = CacheFunctions.AGGREGATE
        # Always check if the pipeline is modifying any collection
        modifying_pipe_info = self._get_database_and_collection_from_modifying_pipeline(
            pipeline
        )
        # Clear the cache if the pipeline is modifying any collection
        if modifying_pipe_info is not None:
            database, coll = modifying_pipe_info
            self._cache_backend.clear_cache_for_database_and_collection(database, coll)

        # If the aggregate function is not in the functions to cache, then just return the result of the regular
        # aggregate. Also, if the pipeline is modifying any collection, then we cannot cache the result or retrieve
        # the result from the cache
        if (
            not self._check_caching_allowed(function_enum) and not cache_always
        ) or modifying_pipe_info is not None:
            return Collection(self.database, self.name).aggregate(
                pipeline, session=session, let=let, comment=comment, **kwargs
            )

        # If the pipeline is modifying any collection, then we cannot cache the result
        # or retrieve the result from the cache
        pipeline_query_info = QueryInfo(function_enum.name, pipeline=pipeline)
        item = self._cache_backend.get(pipeline_query_info)
        if item is not None:
            return iter(item)
        else:
            start = time.time_ns()
            result = Collection(self.database, self.name).aggregate(
                pipeline, session=session, let=let, comment=comment, **kwargs
            )
            end = time.time_ns()
            exec_in_ms = (end - start) / 1e6
            self._cache_backend.set(pipeline_query_info, list(result), exec_in_ms)
            return iter(self._cache_backend.get(pipeline_query_info))

    def insert_many(
        self,
        documents: Iterable[Union[_DocumentType, RawBSONDocument]],
        ordered: bool = True,
        bypass_document_validation: bool = False,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
    ) -> InsertManyResult:
        """Insert an iterable of documents."""

        # Override the insert_many function, such that we can clear the cache
        self._cache_backend.clear()

        return Collection(self.database, self.name).insert_many(
            documents,
            ordered=ordered,
            bypass_document_validation=bypass_document_validation,
            session=session,
            comment=comment,
        )

    def insert_one(
        self,
        document: Union[_DocumentType, RawBSONDocument],
        bypass_document_validation: bool = False,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
    ) -> InsertOneResult:
        """Insert a single document."""

        # Override the insert_one function, such that we can clear the cache
        self._cache_backend.clear()

        return Collection(self.database, self.name).insert_one(
            document,
            bypass_document_validation=bypass_document_validation,
            session=session,
            comment=comment,
        )

    def update_one(
        self,
        filter: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
        upsert: bool = False,
        bypass_document_validation: bool = False,
        collation: Optional[_CollationIn] = None,
        array_filters: Optional[Sequence[Mapping[str, Any]]] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> UpdateResult:
        """Update a single document matching the filter."""

        # Override the update_one function, such that we can clear the cache
        self._cache_backend.clear()

        return Collection(self.database, self.name).update_one(
            filter,
            update,
            upsert=upsert,
            bypass_document_validation=bypass_document_validation,
            collation=collation,
            array_filters=array_filters,
            hint=hint,
            session=session,
            let=let,
            comment=comment,
        )

    def update_many(
        self,
        filter: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
        upsert: bool = False,
        array_filters: Optional[Sequence[Mapping[str, Any]]] = None,
        bypass_document_validation: Optional[bool] = None,
        collation: Optional[_CollationIn] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> UpdateResult:
        """Update one or more documents that match the filter."""

        # Override the update_many function, such that we can clear the cache
        self._cache_backend.clear()

        return Collection(self.database, self.name).update_many(
            filter,
            update,
            upsert=upsert,
            array_filters=array_filters,
            bypass_document_validation=bypass_document_validation,
            collation=collation,
            hint=hint,
            session=session,
            let=let,
            comment=comment,
        )

    def delete_many(
        self,
        filter: Mapping[str, Any],
        collation: Optional[_CollationIn] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> DeleteResult:
        """Delete documents in the collection."""

        # Override the delete_many function, such that we can clear the cache
        self._cache_backend.clear()

        return Collection(self.database, self.name).delete_many(
            filter,
            collation=collation,
            hint=hint,
            session=session,
            let=let,
            comment=comment,
        )

    def delete_one(
        self,
        filter: Mapping[str, Any],
        collation: Optional[_CollationIn] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> DeleteResult:
        """Delete a single document in the collection."""

        # Override the delete_one function, such that we can clear the cache
        self._cache_backend.clear()

        return Collection(self.database, self.name).delete_one(
            filter,
            collation=collation,
            hint=hint,
            session=session,
            let=let,
            comment=comment,
        )

    def drop(
        self,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        encrypted_fields: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Drop this collection."""
        # Override the drop function, such that we can clear the cache
        self._cache_backend.clear()

        return Collection(self.database, self.name).drop(
            session=session, comment=comment, encrypted_fields=encrypted_fields
        )

    def find_one_and_delete(
        self,
        filter: Mapping[str, Any],
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        sort: Optional[_IndexList] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> _DocumentType:
        """Find a single document and delete it, returning the document."""
        # Override the find_one_and_delete function, such that we can clear the cache
        self._cache_backend.clear()

        return Collection(self.database, self.name).find_one_and_delete(
            filter,
            projection=projection,
            sort=sort,
            hint=hint,
            session=session,
            let=let,
            comment=comment,
            **kwargs,
        )

    def find_one_and_replace(
        self,
        filter: Mapping[str, Any],
        replacement: Mapping[str, Any],
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        sort: Optional[_IndexList] = None,
        upsert: bool = False,
        return_document: bool = ReturnDocument.BEFORE,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> _DocumentType:
        """Find a single document and replace it, returning either the original or the replaced document."""
        # Override the find_one_and_replace function, such that we can clear the cache
        self._cache_backend.clear()

        return Collection(self.database, self.name).find_one_and_replace(
            filter,
            replacement,
            projection=projection,
            sort=sort,
            upsert=upsert,
            return_document=return_document,
            hint=hint,
            session=session,
            let=let,
            comment=comment,
            **kwargs,
        )

    def find_one_and_update(
        self,
        filter: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        sort: Optional[_IndexList] = None,
        upsert: bool = False,
        return_document: bool = ReturnDocument.BEFORE,
        array_filters: Optional[Sequence[Mapping[str, Any]]] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> _DocumentType:
        """Find a single document and update it, returning either the original or the updated document."""
        # Override the find_one_and_update function, such that we can clear the cache
        self._cache_backend.clear()

        return Collection(self.database, self.name).find_one_and_update(
            filter,
            update,
            projection=projection,
            sort=sort,
            upsert=upsert,
            return_document=return_document,
            array_filters=array_filters,
            hint=hint,
            session=session,
            let=let,
            comment=comment,
            **kwargs,
        )

    def replace_one(
        self,
        filter: Mapping[str, Any],
        replacement: Mapping[str, Any],
        upsert: bool = False,
        bypass_document_validation: bool = False,
        collation: Optional[_CollationIn] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> UpdateResult:
        """Replace a single document matching the filter."""
        # Override the replace_one function, such that we can clear the cache
        self._cache_backend.clear()

        return Collection(self.database, self.name).replace_one(
            filter,
            replacement,
            upsert=upsert,
            bypass_document_validation=bypass_document_validation,
            collation=collation,
            hint=hint,
            session=session,
            let=let,
            comment=comment,
        )

    def _get_database_and_collection_from_modifying_pipeline(
        self,
        pipeline: _Pipeline,
    ) -> Optional[Tuple[str, str]]:
        """Get the database and collection from the modifying pipeline."""

        if len(pipeline) == 0:
            return None

        # $out and $merge must be the last stages in the pipeline
        last_stage = pipeline[-1]
        if last_stage is None:
            return None

        if "$out" in last_stage:
            last_stage = last_stage["$out"]
        elif "$merge" in last_stage and "into" in last_stage["$merge"]:
            last_stage = last_stage["$merge"]["into"]
        else:
            return None

        if isinstance(last_stage, str):
            return self.database.name, last_stage

        if isinstance(last_stage, dict) and "db" in last_stage and "coll" in last_stage:
            return last_stage["db"], last_stage["coll"]

        return None
