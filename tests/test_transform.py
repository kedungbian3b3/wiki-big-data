import unittest
from datetime import timezone

from common.transform import normalize_wikimedia_recentchange


class TestWikimediaTransform(unittest.TestCase):
    def test_normalize_basic_event(self):
        sample = {
            "id": 123,
            "timestamp": 1710000000,
            "wiki": "enwiki",
            "server_name": "en.wikipedia.org",
            "server_url": "https://en.wikipedia.org",
            "title": "Data engineering",
            "type": "edit",
            "namespace": 0,
            "user": "ExampleUser",
            "bot": False,
            "minor": True,
            "length": {"old": 100, "new": 130},
            "revision": {"old": 10, "new": 11},
            "comment": "test edit",
            "meta": {"domain": "en.wikipedia.org"},
        }

        row = normalize_wikimedia_recentchange(sample)

        self.assertEqual(row["event_id"], "enwiki:123")
        self.assertEqual(row["wiki"], "enwiki")
        self.assertEqual(row["bytes_delta"], 30)
        self.assertEqual(row["rev_new"], 11)
        self.assertEqual(row["is_bot"], False)
        self.assertEqual(row["is_minor"], True)
        self.assertEqual(row["event_time"].tzinfo, timezone.utc)
        self.assertIn("/wiki/Data_engineering", row["page_url"])

    def test_missing_length_does_not_fail(self):
        sample = {
            "meta": {"id": "abc", "dt": "2024-01-01T00:00:00Z"},
            "wiki": "viwiki",
            "title": "Trang chủ",
        }
        row = normalize_wikimedia_recentchange(sample)
        self.assertEqual(row["event_id"], "abc")
        self.assertIsNone(row["bytes_delta"])
        self.assertEqual(row["wiki"], "viwiki")


if __name__ == "__main__":
    unittest.main()
