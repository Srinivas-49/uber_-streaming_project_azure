"""Create real local replay evidence with no Event Hubs or cloud cost."""
import argparse
import json
from uber_event_streaming.replay import fixture, replay

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--rides", type=int, default=30)
    args = p.parse_args()
    outcome = replay(fixture(args.rides))
    print(json.dumps({"metrics": outcome["metrics"],
                      "quarantine": outcome["quarantine"],
                      "sample_ride": outcome["rides"].get("ride-0")}, indent=2))
