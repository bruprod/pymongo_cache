from dataclasses import dataclass
from datetime import datetime
from typing import Any

from cache_backend.Constants import QUERY_INFO
from cache_backend.QueryInfo import QueryInfo


@dataclass()
class CacheEntry:
    query_info: QueryInfo
    value: Any
    collection_name: str
    hash_val: int
    execution_time: int  # in milliseconds
    timestamp: datetime = None
    access_count: int = 0

    def __post_init__(self):
        """Initialize the cache entry."""
        self.timestamp = datetime.now()

    def to_dict(self):
        """Convert the entry to dict"""
        entry_dict = self.__dict__

        entry_dict[QUERY_INFO] = entry_dict[QUERY_INFO].__dict__

        return entry_dict
