import unittest
import configparser
import os
import sqlite3
from unittest.mock import patch, MagicMock
from src.database import Database

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.config_path = "test_config.ini"
        config = configparser.ConfigParser()
        config['DATABASE'] = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }
        with open(self.config_path, 'w') as f:
            config.write(f)

        self.db = Database(config_path=self.config_path)
        self.db.connection = sqlite3.connect(':memory:')
        self.db.cursor = self.db.connection.cursor()

    def tearDown(self):
        if os.path.exists(self.config_path):
            os.remove(self.config_path)
        self.db.disconnect()

    def test_connect_reads_conf(self):
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.return_value.cursor.return_value = MagicMock()
            db = Database(config_path=self.config_path)
            db.connect()
            mock_connect.assert_called_with(
                host='localhost',
                port='5432',
                database='test_db',
                user='test_user',
                password='test_pass'
            )

    def test_execute_query(self):
        self.db.cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        self.db.cursor.execute("INSERT INTO test (name) VALUES ('test')")
        self.db.connection.commit()
        results = self.db.execute_query("SELECT * FROM test")
        self.assertEqual(results, [(1, 'test')])

if __name__ == '__main__':
    unittest.main()
