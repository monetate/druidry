"""Aggregation tests."""

from .context import druidry
import unittest


class TestClient(unittest.TestCase):

    def test_create_client(self):
        druidry.client.Client('localhost', 9999)

    def test_client_endpoint(self):
        client = druidry.client.Client('localhost', 9999)
        self.assertEqual(client.endpoint, 'http://localhost:9999/druid/v2')

    def test_client_broker_metadata_endpoint(self):
        client = druidry.client.Client('localhost', 9999)
        self.assertEqual(client.broker_metadata_endpoint, 'http://localhost:9999/druid/v2/datasources')

    def test_client_data_source_metadata_endpoint(self):
        client = druidry.client.Client('localhost', 9999, data_source='users')
        self.assertEqual(client.data_source_metadata_endpoint, 'http://localhost:9999/druid/v2/datasources/users')

    def test_fetch_schema_fail(self):
        client = druidry.client.Client('localhost', 9999)
        with self.assertRaises(druidry.errors.DruidQueryError):
            client.fetch_schema()
