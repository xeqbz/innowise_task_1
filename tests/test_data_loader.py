import unittest
import json
import os
import sqlite3
import logging
from io import StringIO
from src.data_loader import DataLoader
from src.database import Database

class TestDataLoader(unittest.TestCase):
    def setUp(self):
        self.rooms_path = "tests/data/test_rooms.json"
        self.students_path = "tests/data/test_students.json"
        self.invalid_json_path = "tests/data/invalid.json"
        self.empty_json_path = "tests/data/empty.json"
        os.makedirs("tests/data", exist_ok=True)

        rooms = [
            {"id": 1, "name": "Room 1"},
            {"id": 2, "name": "Room 2"}
        ]
        students = [
            {"id": 1, "name": "Alice", "room": 1, "sex": "F", "birthday": "2000-01-01T00:00:00"},
            {"id": 2, "name": "Bob", "room": 1, "sex": "M", "birthday": "2000-02-01T00:00:00"},
            {"id": 3, "name": "Charlie", "room": 2, "sex": "M", "birthday": "1999-01-01T00:00:00"}
        ]
        with open(self.rooms_path, 'w') as f:
            json.dump(rooms, f)
        with open(self.students_path, 'w') as f:
            json.dump(students, f)
        with open(self.empty_json_path, 'w') as f:
            json.dump([], f)
        with open(self.invalid_json_path, 'w') as f:
            f.write("{")

        self.db = Database(config_path="dummy.ini")
        self.db.connection = sqlite3.connect(':memory:')
        self.db.cursor = self.db.connection.cursor()

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

        self.loader = DataLoader(self.db)

        self.log_stream = StringIO()
        handler = logging.StreamHandler(self.log_stream)
        logger = logging.getLogger('src.data_loader')
        logger.setLevel(logging.ERROR)
        logger.addHandler(handler)
        self.logger = logger

    def tearDown(self):
        self.db.disconnect()
        for path in [self.rooms_path, self.students_path, self.invalid_json_path, self.empty_json_path]:
            if os.path.exists(path):
                os.remove(path)
        if os.path.exists("tests/data"):
            os.rmdir("tests/data")
        self.logger.handlers = []

    def test_load_data_success(self):
        self.loader._insert_rooms = lambda rooms: [
            self.db.execute_query(
                "INSERT OR IGNORE INTO rooms (id, name) VALUES (?, ?)",
                (room["id"], room["name"])
            ) for room in rooms
        ]
        self.loader._insert_students = lambda students: [
            self.db.execute_query(
                "INSERT OR IGNORE INTO students (id, name, room_id, sex, birthday) VALUES (?, ?, ?, ?, ?)",
                (student["id"], student["name"], student["room"], student["sex"], student["birthday"])
            ) for student in students
        ]
        self.loader.load_data(self.rooms_path, self.students_path)
        rooms = self.db.execute_query("SELECT id, name FROM rooms ORDER BY id")
        students = self.db.execute_query("SELECT id, name, room_id, sex, birthday FROM students ORDER BY id")
        self.assertEqual(rooms, [(1, "Room 1"), (2, "Room 2")])
        self.assertEqual(students, [
            (1, "Alice", 1, "F", "2000-01-01T00:00:00"),
            (2, "Bob", 1, "M", "2000-02-01T00:00:00"),
            (3, "Charlie", 2, "M", "1999-01-01T00:00:00")
        ])

    def test_on_conflict_do_nothing_rooms(self):
        self.loader._insert_rooms = lambda rooms: [
            self.db.execute_query(
                "INSERT OR IGNORE INTO rooms (id, name) VALUES (?, ?)",
                (room["id"], room["name"])
            ) for room in rooms
        ]
        self.loader._insert_students = lambda students: []
        self.loader.load_data(self.rooms_path, self.students_path)
        self.loader.load_data(self.rooms_path, self.students_path)  # Duplicate load
        rooms = self.db.execute_query("SELECT id, name FROM rooms ORDER BY id")
        self.assertEqual(len(rooms), 2)
        self.assertEqual(rooms, [(1, "Room 1"), (2, "Room 2")])

    def test_on_conflict_do_nothing_students(self):
        self.loader._insert_rooms = lambda rooms: [
            self.db.execute_query(
                "INSERT OR IGNORE INTO rooms (id, name) VALUES (?, ?)",
                (room["id"], room["name"])
            ) for room in rooms
        ]
        self.loader._insert_students = lambda students: [
            self.db.execute_query(
                "INSERT OR IGNORE INTO students (id, name, room_id, sex, birthday) VALUES (?, ?, ?, ?, ?)",
                (student["id"], student["name"], student["room"], student["sex"], student["birthday"])
            ) for student in students
        ]
        self.loader.load_data(self.rooms_path, self.students_path)
        self.loader.load_data(self.rooms_path, self.students_path)  # Duplicate load
        students = self.db.execute_query("SELECT id, name FROM students ORDER BY id")
        self.assertEqual(len(students), 3)
        self.assertEqual(students, [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie")
        ])

    def test_read_json_invalid_file(self):
        with self.assertRaises(json.JSONDecodeError):
            self.loader._read_json(self.invalid_json_path)
        self.log_stream.seek(0)
        log_output = self.log_stream.read()
        self.assertIn("Failed to load tests/data/invalid.json", log_output)

    def test_read_json_missing_file(self):
        with self.assertRaises(FileNotFoundError):
            self.loader._read_json("nonexistent.json")
        self.log_stream.seek(0)
        log_output = self.log_stream.read()
        self.assertIn("Failed to load nonexistent.json", log_output)

    def test_read_json_empty_file(self):
        data = self.loader._read_json(self.empty_json_path)
        self.assertEqual(data, [])

    def test_insert_rooms_missing_field(self):
        invalid_rooms = [{"name": "Room 3"}]
        with open(self.rooms_path, 'w') as f:
            json.dump(invalid_rooms, f)
        with self.assertRaises(KeyError):
            self.loader.load_data(self.rooms_path, self.students_path)
        self.log_stream.seek(0)
        log_output = self.log_stream.read()
        self.assertIn("Failed to insert {'name': 'Room 3'}", log_output)

if __name__ == '__main__':
    unittest.main()