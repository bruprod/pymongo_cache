import unittest
from unittest.mock import patch, MagicMock

from pymongo.collection import Collection

from cache_backend.CacheBackend import CacheBackend
from pymongo_wrappers.MongoClientWithCache import MongoClientWithCache
from pymongo_wrappers.MongoCollectionWithCache import MongoCollectionWithCache
from pymongo_wrappers.MongoDatabaseWithCache import MongoDatabaseWithCache


class TestMongoCollectionWithCacheCacheInteraction(unittest.TestCase):
    def setUp(self):
        self.client = MongoClientWithCache()
        self.database = MongoDatabaseWithCache(self.client, "test")
        self.collection = MongoCollectionWithCache(
            self.database, "test", cache_backend=CacheBackend.IN_MEMORY
        )

    @patch.object(Collection, "find_one")
    def test_cache_retrieval_on_find_one(self, mock_find_one: MagicMock):
        self.collection._cache_backend = MagicMock()
        self.collection._cache_backend.get.return_value = {"_id": 1, "name": "test"}

        # Call should hit the cache because the mocked get function returns a value
        result = self.collection.find_one({"_id": 1})
        mock_find_one.assert_not_called()
        self.collection._cache_backend.get.assert_called_once()
        self.assertEqual(result, {"_id": 1, "name": "test"})

    @patch.object(Collection, "insert_one")
    def test_cache_clearing_on_insert_one(self, mock_insert_one):
        self.collection._cache_backend = MagicMock()
        mock_insert_one.return_value = {"_id": 1, "name": "test"}
        self.collection.insert_one({"_id": 1, "name": "test"})
        self.collection._cache_backend.clear.assert_called_once()
        self.collection._cache_backend.get.assert_not_called()

    @patch.object(Collection, "update_one")
    def test_cache_clearing_on_update_one(self, mock_update_one):
        self.collection._cache_backend = MagicMock()
        mock_update_one.return_value = {"_id": 1, "name": "test"}
        self.collection.update_one({"_id": 1}, {"$set": {"name": "test"}})
        self.collection._cache_backend.clear.assert_called_once()
        self.collection._cache_backend.get.assert_not_called()

    @patch.object(Collection, "delete_one")
    def test_cache_clearing_on_delete_one(self, mock_delete_one):
        self.collection._cache_backend = MagicMock()
        mock_delete_one.return_value = {"_id": 1, "name": "test"}
        self.collection.delete_one({"_id": 1})
        self.collection._cache_backend.clear.assert_called_once()
        self.collection._cache_backend.get.assert_not_called()

    @patch.object(Collection, "delete_many")
    def test_cache_clearing_on_delete_many(self, mock_delete_many):
        self.collection._cache_backend = MagicMock()
        mock_delete_many.return_value = {"_id": 1, "name": "test"}
        self.collection.delete_many({"_id": 1})
        self.collection._cache_backend.clear.assert_called_once()
        self.collection._cache_backend.get.assert_not_called()

    @patch.object(Collection, "replace_one")
    def test_cache_clearing_on_replace_one(self, mock_replace_one):
        self.collection._cache_backend = MagicMock()
        mock_replace_one.return_value = {"_id": 1, "name": "test"}
        self.collection.replace_one({"_id": 1}, {"_id": 1, "name": "test"})
        self.collection._cache_backend.clear.assert_called_once()
        self.collection._cache_backend.get.assert_not_called()

    @patch.object(Collection, "aggregate")
    def test_cache_clearing_not_called_non_modifying_aggregate(self, mock_aggregate):
        self.collection._cache_backend = MagicMock()
        mock_aggregate.return_value = {"_id": 1, "name": "test"}
        self.collection.aggregate([{"$match": {"_id": 1}}])
        self.collection._cache_backend.clear.assert_not_called()
        self.collection._cache_backend.get.assert_called_once()

    @patch.object(Collection, "aggregate")
    def test_cache_clearing_modifying_aggregate(self, mock_aggregate):
        self.collection._cache_backend = MagicMock()
        mock_aggregate.return_value = {"_id": 1, "name": "test"}
        self.collection.aggregate([{"$out": "test"}])
        self.collection._cache_backend.clear_cache_for_database_and_collection.assert_called_once()
        self.collection._cache_backend.get.assert_not_called()

    def test_cache_retrieval_on_cache_always(self):
        # Set functions to cache to an empty list to simulate caching no functions
        self.collection._functions_to_cache = []
        self.collection._cache_backend = MagicMock()
        self.collection._cache_backend.get.return_value = {"_id": 1, "name": "test"}

        result = self.collection.find_one({"_id": 1}, cache_always=True)
        self.collection._cache_backend.get.assert_called_once()
        self.assertEqual(result, {"_id": 1, "name": "test"})

        self.collection._cache_backend.reset_mock()
        self.collection._cache_backend.get.return_value = [{"_id": 1, "name": "test"}]
        result = list(self.collection.find({"_id": 1}, cache_always=True))
        self.collection._cache_backend.get.assert_called_once()
        self.assertEqual(result, [{"_id": 1, "name": "test"}])

    @patch.object(Collection, "find")
    def test_cache_retrieval_no_caching_functions_and_cache_always_false_find(
        self, mock_find
    ):
        # Set functions to cache to an empty list to simulate caching no functions
        self.collection._functions_to_cache = []
        self.collection._cache_backend = MagicMock()
        self.collection._cache_backend.get.return_value = {"_id": 1, "name": "test"}

        mock_find.return_value = [{"_id": 1, "name": "test"}]

        self.collection._cache_backend.get.return_value = None
        result = list(self.collection.find({"_id": 1}, cache_always=False))
        self.collection._cache_backend.get.assert_not_called()
        self.assertEqual(result, [{"_id": 1, "name": "test"}])

    @patch.object(Collection, "find_one")
    def test_cache_retrieval_no_caching_functions_and_cache_always_false_find_one(
        self, mock_find_one
    ):
        # Set functions to cache to an empty list to simulate caching no functions
        self.collection._functions_to_cache = []
        self.collection._cache_backend = MagicMock()
        self.collection._cache_backend.get.return_value = {"_id": 1, "name": "test"}

        mock_find_one.return_value = {"_id": 1, "name": "test"}

        self.collection._cache_backend.get.return_value = None
        result = self.collection.find_one({"_id": 1}, cache_always=False)
        self.collection._cache_backend.get.assert_not_called()
        self.assertEqual(result, {"_id": 1, "name": "test"})

    @patch.object(Collection, "find")
    def test_cache_retrieval_on_find(self, mock_find):
        self.collection._cache_backend = MagicMock()
        self.collection._cache_backend.get.return_value = [
            {"_id": 1, "name": "test_cache"}
        ]

        # Call should hit the cache because the mocked get function returns a value
        result = list(self.collection.find({"_id": 1}))
        mock_find.assert_not_called()
        self.collection._cache_backend.get.assert_called_once()
        self.assertEqual(result, [{"_id": 1, "name": "test_cache"}])

        # Call should not hit the cache because the mocked get function returns None
        self.collection._cache_backend.get.side_effect = [
            None,
            [{"_id": 1, "name": "test_no_cache"}],
        ]
        mock_find.return_value = [{"_id": 1, "name": "test_no_cache"}]
        result = list(self.collection.find({"_id": 1}))
        mock_find.assert_called_once()
        self.assertEqual(result, [{"_id": 1, "name": "test_no_cache"}])


if __name__ == "__main__":
    unittest.main()
