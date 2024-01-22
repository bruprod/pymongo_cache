import unittest
from datetime import datetime

from pymongo import MongoClient

from cache_backend.CacheBackend import CacheBackend
from pymongo_wrappers.MongoClientWithCache import MongoClientWithCache


class TestMongoCollectionWithCacheAgainstRegularResults(unittest.TestCase):
    def setUp(self):
        self.regular_client = MongoClient()
        self.regular_db = self.regular_client["test"]
        self.regular_collection = self.regular_db["test"]
        self.cache_client = MongoClientWithCache(cache_backend=CacheBackend.IN_MEMORY)
        self.cache_db = self.cache_client["test"]
        self.cache_collection = self.cache_db["test"]

        self.regular_collection.insert_one(
            {
                "ticker_name": "AAPL",
                "stock_price": 100,
                "timestamp": datetime(2020, 1, 1),
            }
        )
        self.regular_collection.insert_one(
            {
                "ticker_name": "AAPL",
                "stock_price": 200,
                "timestamp": datetime(2021, 1, 1),
            }
        )

        self.regular_collection.insert_one(
            {
                "ticker_name": "AAPL",
                "stock_price": 300,
                "timestamp": datetime(2023, 1, 1),
            }
        )

        self.regular_collection.insert_one(
            {
                "ticker_name": "MSFT",
                "stock_price": 250,
                "timestamp": datetime(2023, 1, 1),
            }
        )

    def tearDown(self):
        self.regular_collection.drop()
        self.cache_collection.drop()

    def test_find_one(self):
        self.cache_collection.find_one({"ticker_name": "MSFT"})
        cache_dict = self.cache_collection.find_one({"ticker_name": "MSFT"})
        regular_dict = self.regular_collection.find_one({"ticker_name": "MSFT"})

        self.assertDictEqual(cache_dict, regular_dict)

    def test_find_one_sort(self):
        self.cache_collection.find_one(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)]
        )
        cache_dict = self.cache_collection.find_one(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)]
        )
        regular_dict = self.regular_collection.find_one(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)]
        )

        self.assertDictEqual(cache_dict, regular_dict)

    def test_find_one_sort_skip(self):
        self.cache_collection.find_one(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)], skip=1
        )
        cache_dict = self.cache_collection.find_one(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)], skip=1
        )
        regular_dict = self.regular_collection.find_one(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)], skip=1
        )

        self.assertDictEqual(cache_dict, regular_dict)

    def test_find(self):
        self.cache_collection.find({"ticker_name": "AAPL"})
        cache_entries = self.cache_collection.find({"ticker_name": "AAPL"})
        regular_entries = self.regular_collection.find({"ticker_name": "AAPL"})

        self.assertListEqual(list(cache_entries), list(regular_entries))

    def test_find_sort(self):
        self.cache_collection.find({"ticker_name": "AAPL"}, sort=[("timestamp", -1)])
        cache_entries = self.cache_collection.find(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)]
        )
        regular_entries = self.regular_collection.find(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)]
        )

        self.assertListEqual(list(cache_entries), list(regular_entries))

    def test_find_limit(self):
        self.cache_collection.find({"ticker_name": "AAPL"}, limit=2)
        cache_entries = self.cache_collection.find({"ticker_name": "AAPL"}, limit=2)
        regular_entries = self.regular_collection.find({"ticker_name": "AAPL"}, limit=2)

        self.assertListEqual(list(cache_entries), list(regular_entries))

    def test_find_sort_limit(self):
        self.cache_collection.find(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)], limit=2
        )
        cache_entries = self.cache_collection.find(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)], limit=2
        )
        regular_entries = self.regular_collection.find(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)], limit=2
        )

        self.assertListEqual(list(cache_entries), list(regular_entries))

    def test_find_sort_limit_skip(self):
        self.cache_collection.find(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)], limit=2, skip=1
        )
        cache_entries = self.cache_collection.find(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)], limit=2, skip=1
        )
        regular_entries = self.regular_collection.find(
            {"ticker_name": "AAPL"}, sort=[("timestamp", -1)], limit=2, skip=1
        )

        self.assertListEqual(list(cache_entries), list(regular_entries))

    def test_find_sort_limit_skip_projection(self):
        self.cache_collection.find(
            {"ticker_name": "AAPL"},
            sort=[("timestamp", -1)],
            limit=2,
            skip=1,
            projection={"ticker_name": 1},
        )
        cache_entries = self.cache_collection.find(
            {"ticker_name": "AAPL"},
            sort=[("timestamp", -1)],
            limit=2,
            skip=1,
            projection={"ticker_name": 1},
        )
        regular_entries = self.regular_collection.find(
            {"ticker_name": "AAPL"},
            sort=[("timestamp", -1)],
            limit=2,
            skip=1,
            projection={"ticker_name": 1},
        )

        self.assertListEqual(list(cache_entries), list(regular_entries))

    def test_aggregate(self):
        self.cache_collection.aggregate([{"$match": {"ticker_name": "AAPL"}}])
        cache_entries = self.cache_collection.aggregate(
            [{"$match": {"ticker_name": "AAPL"}}]
        )
        regular_entries = self.regular_collection.aggregate(
            [{"$match": {"ticker_name": "AAPL"}}]
        )

        self.assertListEqual(list(cache_entries), list(regular_entries))

    def test_aggregate_sort(self):
        self.cache_collection.aggregate(
            [{"$match": {"ticker_name": "AAPL"}}, {"$sort": {"timestamp": -1}}]
        )
        cache_entries = self.cache_collection.aggregate(
            [{"$match": {"ticker_name": "AAPL"}}, {"$sort": {"timestamp": -1}}]
        )
        regular_entries = self.regular_collection.aggregate(
            [{"$match": {"ticker_name": "AAPL"}}, {"$sort": {"timestamp": -1}}]
        )

        self.assertListEqual(list(cache_entries), list(regular_entries))

    def test_aggregate_sort_limit(self):
        self.cache_collection.aggregate(
            [
                {"$match": {"ticker_name": "AAPL"}},
                {"$sort": {"timestamp": -1}},
                {"$limit": 2},
            ]
        )
        cache_entries = self.cache_collection.aggregate(
            [
                {"$match": {"ticker_name": "AAPL"}},
                {"$sort": {"timestamp": -1}},
                {"$limit": 2},
            ]
        )
        regular_entries = self.regular_collection.aggregate(
            [
                {"$match": {"ticker_name": "AAPL"}},
                {"$sort": {"timestamp": -1}},
                {"$limit": 2},
            ]
        )

        self.assertListEqual(list(cache_entries), list(regular_entries))

    def test_aggregate_sort_group(self):
        self.cache_collection.aggregate(
            [
                {"$match": {"ticker_name": "AAPL"}},
                {"$sort": {"timestamp": -1}},
                {"$group": {"_id": "$ticker_name", "count": {"$sum": 1}}},
            ]
        )
        cache_entries = self.cache_collection.aggregate(
            [
                {"$match": {"ticker_name": "AAPL"}},
                {"$sort": {"timestamp": -1}},
                {"$group": {"_id": "$ticker_name", "count": {"$sum": 1}}},
            ]
        )
        regular_entries = self.regular_collection.aggregate(
            [
                {"$match": {"ticker_name": "AAPL"}},
                {"$sort": {"timestamp": -1}},
                {"$group": {"_id": "$ticker_name", "count": {"$sum": 1}}},
            ]
        )

        self.assertListEqual(list(cache_entries), list(regular_entries))

    def test_aggregate_find_find_one(self):
        self.cache_collection.find({"ticker_name": "AAPL"})
        cache_entries_find = self.cache_collection.find({"ticker_name": "AAPL"})
        regular_entries_find = self.regular_collection.find({"ticker_name": "AAPL"})

        self.cache_collection.find_one({"ticker_name": "AAPL"})
        cache_entries_find_one = self.cache_collection.find_one({"ticker_name": "AAPL"})
        regular_entries_find_one = self.regular_collection.find_one(
            {"ticker_name": "AAPL"}
        )

        self.cache_collection.aggregate(
            [
                {"$match": {"ticker_name": "AAPL"}},
                {"$sort": {"timestamp": -1}},
                {"$group": {"_id": "$ticker_name", "count": {"$sum": 1}}},
            ]
        )
        cache_entries_aggregate = self.cache_collection.aggregate(
            [
                {"$match": {"ticker_name": "AAPL"}},
                {"$sort": {"timestamp": -1}},
                {"$group": {"_id": "$ticker_name", "count": {"$sum": 1}}},
            ]
        )
        regular_entries_aggregate = self.regular_collection.aggregate(
            [
                {"$match": {"ticker_name": "AAPL"}},
                {"$sort": {"timestamp": -1}},
                {"$group": {"_id": "$ticker_name", "count": {"$sum": 1}}},
            ]
        )

        self.assertListEqual(list(cache_entries_find), list(regular_entries_find))
        self.assertDictEqual(cache_entries_find_one, regular_entries_find_one)
        self.assertListEqual(
            list(cache_entries_aggregate), list(regular_entries_aggregate)
        )


if __name__ == "__main__":
    unittest.main()
