"""Mongo database class with cache."""
from pymongo.database import Database

from MongoCollectionWithCache import MongoCollectionWithCache


class MongoDatabaseWithCache(Database):
    """
    Mongo client class with cache.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __getitem__(self, item):
        return MongoCollectionWithCache(self, item)
