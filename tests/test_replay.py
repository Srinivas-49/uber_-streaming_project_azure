import unittest
from decimal import Decimal
from uber_event_streaming.replay import fixture, normalize, replay

class ReplayContractTests(unittest.TestCase):
    def test_idempotent_duplicates_and_late_events(self):
        events = fixture(30)
        result = replay(events)
        m = result["metrics"]
        self.assertEqual(m["received"], 39)
        self.assertEqual(m["accepted_events"], 37)
        self.assertEqual(m["duplicate_events"], 1)
        self.assertEqual(m["invalid_events"], 1)
        self.assertEqual(m["stale_events"], 1)
        self.assertEqual(m["updated_rides"], 6)
        self.assertEqual(m["current_rides"], 30)
        self.assertEqual(m["completed_rides"], 6)
        self.assertEqual(result["rides"]["ride-0"]["status"], "completed")
        self.assertEqual(result["quarantine"], [{"position": 38, "reason": "invalid event_time"}])

    def test_out_of_order_results_are_stable(self):
        events = fixture(5)
        late_first = [events[-2], *events[:-2], events[-1]]
        self.assertEqual(replay(events)["rides"], replay(late_first)["rides"])

    def test_strict_quality_and_pii_safe_quarantine(self):
        event = fixture(1)[0]
        for patch in ({"fare_gbp": "NaN"}, {"fare_gbp": "-2"},
                      {"event_time": "2026-11-14T11:00:00"},
                      {"status": "other"}):
            with self.assertRaises(ValueError):
                normalize({**event, **patch})
        unsafe = {**event, "event_time": "bad", "passenger_email": "private@example.org"}
        self.assertNotIn("private@example.org", str(replay([unsafe])))

    def test_currency_exactness(self):
        ev = fixture(1)[0]
        self.assertEqual(normalize({**ev, "fare_gbp": "10.20"})["fare_gbp"], Decimal("10.20"))
        with self.assertRaises(ValueError):
            fixture(0)

if __name__ == "__main__":
    unittest.main()
