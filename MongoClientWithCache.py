"""Mongo client class with cache."""
from pymongo import MongoClient

from MongoDatabaseWithCache import MongoDatabaseWithCache


class MongoClientWithCache(MongoClient):
    """
    Mongo client class with cache.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __getitem__(self, name: str) -> MongoDatabaseWithCache:
        return MongoDatabaseWithCache(self, name)
