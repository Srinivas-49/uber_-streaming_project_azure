import unittest
from pyspark.sql import SparkSession
from uber_event_streaming.quality import assert_ride_quality, quality_metrics, validate_schema

class QualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("quality-tests")
                     .config("spark.ui.enabled", "false").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_valid(self):
        df = self.spark.createDataFrame([("r1", "2026-01-01"), ("r2", "2026-01-02")],
                                        ["ride_id", "booking_timestamp"])
        self.assertEqual(assert_ride_quality(df)["row_count"], 2)

    def test_duplicate(self):
        df = self.spark.createDataFrame([("r1", "2026-01-01"), ("r1", "2026-01-02")],
                                        ["ride_id", "booking_timestamp"])
        self.assertEqual(quality_metrics(df)["duplicate_ride_ids"], 1)
        with self.assertRaises(ValueError):
            assert_ride_quality(df)

    def test_null(self):
        df = self.spark.createDataFrame([("r1", "2026-01-01"), (None, "2026-01-02")],
                                        ["ride_id", "booking_timestamp"])
        with self.assertRaises(ValueError):
            assert_ride_quality(df)

    def test_missing_column(self):
        df = self.spark.createDataFrame([("r1",)], ["ride_id"])
        with self.assertRaises(ValueError):
            validate_schema(df)

if __name__ == "__main__":
    unittest.main()
