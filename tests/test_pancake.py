"""Unit tests for Pancake response parsing and contact normalisation."""

import sys
import unittest
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1] / "app"
sys.path.insert(0, str(APP_DIR))

from common.pancake_api import PancakeAPIClient, PancakeAPIError
from pancake.pancake_customers_ingestion import merge_customer, normalize_customer
from pancake.pancake_shops_ingestion import extract_shop_phone_numbers


class StubPancakeClient(PancakeAPIClient):
    def __init__(self, payload):
        self.payload = payload
        self.user_access_token = "test-token"

    def _request(self, *args, **kwargs):
        return self.payload


class PancakeAPIParsingTests(unittest.TestCase):
    def test_orders_are_read_from_data_key(self):
        client = StubPancakeClient({"success": True, "data": [{"id": 1}]})
        self.assertEqual(client.get_orders("shop-1"), [{"id": 1}])

    def test_customers_are_read_from_data_key(self):
        client = StubPancakeClient({"success": True, "data": [{"id": "c-1"}]})
        self.assertEqual(client.get_customers("shop-1"), [{"id": "c-1"}])

    def test_pages_are_flattened_from_categorized_shape(self):
        client = StubPancakeClient(
            {
                "success": True,
                "categorized": {
                    "activated": [{"id": "p-1"}],
                    "inactivated": [{"id": "p-2"}],
                    "hidden": [],
                    "nopermission": [],
                },
            }
        )
        self.assertEqual([page["id"] for page in client.get_pages()], ["p-1", "p-2"])

    def test_success_false_is_not_silently_treated_as_empty(self):
        client = StubPancakeClient({"success": False, "message": "invalid token"})
        with self.assertRaises(PancakeAPIError):
            client.get_customers("shop-1")


class PancakeContactTests(unittest.TestCase):
    def test_shop_phones_are_read_from_connected_pages(self):
        shop = {
            "id": "shop-1",
            "phone": "",
            "pages": [
                {"id": "page-1", "phone_number": "0901000001"},
                {"phone_number": "0901000002"},
                {"phone_number": "0901000001"},
            ],
        }
        managed_pages = [
            {
                "id": "page-1",
                "shop_id": "shop-1",
                "platform_extra_info": {"phone_number": "0901000003"},
            }
        ]
        self.assertEqual(
            extract_shop_phone_numbers(shop, managed_pages),
            ["0901000001", "0901000002", "0901000003"],
        )

    def test_nested_pos_contacts_are_preserved(self):
        record = normalize_customer(
            {
                "id": "page-customer-1",
                "name": "Customer",
                "page_id": "page-1",
                "shop_customer": {
                    "id": "shop-customer-1",
                    "shop_id": "shop-1",
                    "phone_numbers": ["0901000001", "0901000002"],
                    "emails": ["customer@example.com"],
                    "gender": "female",
                    "inserted_at": "2026-01-01T00:00:00",
                    "updated_at": "2026-01-02T00:00:00",
                },
            }
        )
        self.assertIsNotNone(record)
        self.assertEqual(record["phone_numbers"], ["0901000001", "0901000002"])
        self.assertEqual(record["emails"], ["customer@example.com"])
        self.assertEqual(record["gender"], "female")

    def test_page_customer_contacts_are_merged_across_pages(self):
        customers = {}
        first = normalize_customer(
            {
                "id": "pc-1",
                "customer_id": "global-1",
                "name": "Customer",
                "phone_numbers": ["0901000001"],
                "emails": ["first@example.com"],
                "inserted_at": "2026-01-01T00:00:00",
                "updated_at": "2026-01-02T00:00:00",
            },
            page_id="page-1",
        )
        second = normalize_customer(
            {
                "id": "pc-2",
                "customer_id": "global-1",
                "name": "Customer",
                "phone_numbers": ["0901000002"],
                "emails": ["second@example.com"],
                "inserted_at": "2026-01-01T00:00:00",
                "updated_at": "2026-01-03T00:00:00",
            },
            page_id="page-2",
        )
        merge_customer(customers, first)
        merge_customer(customers, second)
        merged = customers["global-1"]
        self.assertEqual(merged["page_ids"], ["page-1", "page-2"])
        self.assertEqual(merged["phone_numbers"], ["0901000001", "0901000002"])
        self.assertEqual(
            merged["emails"], ["first@example.com", "second@example.com"]
        )


if __name__ == "__main__":
    unittest.main()
