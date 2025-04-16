import unittest
import sqlite3
import json

from src.database import Database
from src.queries import QueryExecutor



class MockCursor:
    def __init__(self, real_cursor, description_names=None):
        self.real_cursor = real_cursor
        self.description_names = description_names

    def execute(self, query, params=()):
        return self.real_cursor.execute(query, params)

    def fetchall(self):
        return self.real_cursor.fetchall()

    @property
    def description(self):
        if self.description_names:
            class Column:
                def __init__(self, name):
                    self.name = name

            return [Column(name) for name in self.description_names]
        return self.real_cursor.description

    def __getattr__(self, name):
        return getattr(self.real_cursor, name)


class TestQueryExecutor(unittest.TestCase):
    def setUp(self):
        # Use in-memory SQLite
        self.db = Database(config_path="dummy.ini")
        self.db.connection = sqlite3.connect(':memory:')
        self.real_cursor = self.db.connection.cursor()
        self.db.cursor = MockCursor(self.real_cursor)

        # Create schema
        self.db.cursor.execute("""
            CREATE TABLE rooms (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        self.db.cursor.execute("""
            CREATE TABLE students (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                room_id INTEGER NOT NULL,
                sex TEXT NOT NULL,
                birthday TEXT NOT NULL,
                FOREIGN KEY (room_id) REFERENCES rooms(id)
            )
        """)
        self.db.connection.commit()

        # Insert sample data
        self.db.cursor.executemany(
            "INSERT INTO rooms (id, name) VALUES (?, ?)",
            [(1, "Room 1"), (2, "Room 2")]
        )
        self.db.cursor.executemany(
            "INSERT INTO students (id, name, room_id, sex, birthday) VALUES (?, ?, ?, ?, ?)",
            [
                (1, "Alice", 1, "F", "2000-01-01"),
                (2, "Bob", 1, "M", "1998-01-01"),
                (3, "Charlie", 2, "M", "1999-01-01")
            ]
        )
        self.db.connection.commit()

        self.executor = QueryExecutor(self.db)

    def tearDown(self):
        self.db.disconnect()

    def test_create_indexes(self):
        self.db.cursor.execute("DROP INDEX IF EXISTS idx_room_id")
        self.db.cursor.execute("DROP INDEX IF EXISTS idx_birthday")
        self.db.cursor.execute("DROP INDEX IF EXISTS idx_sex")

        self.executor._create_indexes()

        self.db.cursor.execute("PRAGMA index_list(students)")
        indexes = [row[1] for row in self.db.cursor.fetchall()]
        self.assertIn("idx_room_id", indexes)
        self.assertIn("idx_birthday", indexes)
        self.assertIn("idx_sex", indexes)

    def test_get_room_student_count(self):
        self.db.cursor.description_names = ["room_id", "name", "student_count"]
        original_query = self.executor.get_room_student_count

        def mock_query(output_format):
            query = """
            SELECT r.id AS room_id, r.name, COUNT(s.id) AS student_count
            FROM rooms r
            LEFT JOIN students s ON r.id = s.room_id
            GROUP BY r.id, r.name
            """
            results = self.db.execute_query(query)
            return self.executor._format_output(results, output_format)

        self.executor.get_room_student_count = mock_query

        result = self.executor.get_room_student_count("json")
        data = json.loads(result)
        expected = [
            {"room_id": 1, "name": "Room 1", "student_count": 2},
            {"room_id": 2, "name": "Room 2", "student_count": 1}
        ]
        self.assertEqual(data, expected)

        self.executor.get_room_student_count = original_query

    def test_get_lowest_avg_age_rooms(self):
        self.db.cursor.description_names = ["room_id", "name", "avg_age"]
        original_query = self.executor.get_lowest_avg_age_rooms

        def mock_query(output_format):
            query = """
            SELECT r.id AS room_id, r.name, 
                AVG((julianday('2025-04-16') - julianday(s.birthday)) / 365.25) AS avg_age
            FROM rooms r
            JOIN students s ON r.id = s.room_id
            GROUP BY r.id, r.name
            ORDER BY avg_age
            LIMIT 5
            """
            results = self.db.execute_query(query)
            return self.executor._format_output(results, output_format)

        self.executor.get_lowest_avg_age_rooms = mock_query

        result = self.executor.get_lowest_avg_age_rooms("json")
        data = json.loads(result)
        expected = [
            {"room_id": 1, "name": "Room 1", "avg_age": 26.3},  # (25.3 + 27.3) / 2
            {"room_id": 2, "name": "Room 2", "avg_age": 26.3}  # 26.3
        ]
        for actual, expect in zip(data, expected):
            self.assertEqual(actual["room_id"], expect["room_id"])
            self.assertEqual(actual["name"], expect["name"])
            self.assertAlmostEqual(actual["avg_age"], expect["avg_age"], places=1)

        self.executor.get_lowest_avg_age_rooms = original_query

    def test_get_highest_age_diff_rooms(self):
        self.db.cursor.description_names = ["room_id", "name", "age_diff"]
        original_query = self.executor.get_highest_age_diff_rooms

        def mock_query(output_format):
            query = """
            SELECT r.id AS room_id, r.name,
                (julianday(MAX(s.birthday)) - julianday(MIN(s.birthday))) / 365.25 AS age_diff
            FROM rooms r
            JOIN students s ON r.id = s.room_id
            GROUP BY r.id, r.name
            HAVING (julianday(MAX(s.birthday)) - julianday(MIN(s.birthday))) > 0
            ORDER BY age_diff DESC
            LIMIT 5
            """
            results = self.db.execute_query(query)
            return self.executor._format_output(results, output_format)

        self.executor.get_highest_age_diff_rooms = mock_query

        result = self.executor.get_highest_age_diff_rooms("json")
        data = json.loads(result)
        expected = [
            {"room_id": 1, "name": "Room 1", "age_diff": 2.0}  # 2000 - 1998
        ]
        for actual, expect in zip(data, expected):
            self.assertEqual(actual["room_id"], expect["room_id"])
            self.assertEqual(actual["name"], expect["name"])
            self.assertAlmostEqual(actual["age_diff"], expect["age_diff"], places=1)

        self.executor.get_highest_age_diff_rooms = original_query

    def test_get_mixed_sex_rooms(self):
        self.db.cursor.description_names = ["room_id", "name"]
        original_query = self.executor.get_mixed_sex_rooms

        def mock_query(output_format):
            query = """
            SELECT r.id AS room_id, r.name
            FROM rooms r
            JOIN students s ON r.id = s.room_id
            GROUP BY r.id, r.name
            HAVING COUNT(DISTINCT s.sex) > 1
            """
            results = self.db.execute_query(query)
            return self.executor._format_output(results, output_format)

        self.executor.get_mixed_sex_rooms = mock_query

        result = self.executor.get_mixed_sex_rooms("json")
        data = json.loads(result)
        expected = [{"room_id": 1, "name": "Room 1"}]
        self.assertEqual(data, expected)

        self.executor.get_mixed_sex_rooms = original_query

    def test_format_output(self):
        self.db.cursor.description_names = ["room_id", "name", "student_count"]
        self.db.cursor.execute("SELECT id AS room_id, name, 0 AS student_count FROM rooms")
        data = [(1, "Room 1", 2)]
        result = self.executor._format_output(data, "json")
        expected = json.dumps([
            {"room_id": 1, "name": "Room 1", "student_count": 2}
        ], indent=2)
        self.assertEqual(result.strip(), expected.strip())


if __name__ == '__main__':
    unittest.main()