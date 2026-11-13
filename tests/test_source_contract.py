import hashlib
import json
import unittest

from weather_bulletins.client import Client
from weather_bulletins.decoding import parse_page, records as decode_records
from weather_bulletins.origin import load_scenario, serve


class SourceContractTests(unittest.TestCase):
    def test_domain_observations_preserve_source_distinctions(self):
        scenario = load_scenario()
        records = scenario["source_records"]
        with serve(scenario) as origin:
            client = Client(origin.base_url)
            self.assertEqual(records[0]["alert"], records[1]["alert"])
            self.assertNotEqual(records[0]["boundary_version"], records[1]["boundary_version"])
            self.assertEqual(records[2]["status"], "cancelled")
            self.assertNotEqual(records[1]["issuer"], records[3]["issuer"])

    def test_every_record_has_the_declared_source_identity(self):
        scenario = load_scenario()
        for record in scenario["source_records"]:
            for field in scenario["identity_fields"]:
                self.assertIn(field, record)
                self.assertNotEqual(str(record[field]).strip(), "")


if __name__ == "__main__":
    unittest.main()
