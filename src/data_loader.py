import json
import logging
from typing import List, Dict
from src.database import Database

logger = logging.getLogger(__name__)

class DataLoader:
    """
    A class responsible for loading room and student data from JSON files into the database.
    """
    def __init__(self, db: 'Database'):
        """
        Initialize the DataLoader with a database instance.

        Args:
            db (Database): An instance of the Database class to interact with the database.
        """
        self.db = db

    def load_data(self, rooms_file: str, students_file: str) -> None:
        """
        Load and insert room and student data into the database from JSON files.

        Args:
            rooms_file (str): Path to the JSON file containing room data.
            students_file (str): Path to the JSON file containing student data.

        Raises:
            Exception: If reading or inserting data fails.
        """
        try:
            rooms = self._read_json(rooms_file)
            students = self._read_json(students_file)

            self._insert_rooms(rooms)
            self._insert_students(students)
            self.db.connection.commit()
        except Exception as e:
            logger.error(f"Error in def load_data: {e}")
            raise

    @staticmethod
    def _read_json(file_path: str) -> List[Dict]:
        """
        Read and parse a JSON file.

        Args:
            file_path (str): Path to the JSON file.

        Returns:
            List[Dict]: List of dictionaries representing parsed JSON objects.

        Raises:
            Exception: If file reading or JSON parsing fails.
        """
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
            raise

    def _insert_rooms(self, rooms: List[Dict]) -> None:
        """
        Insert room records into the database.

        Args:
            rooms (List[Dict]): List of room dictionaries containing 'id' and 'name'.

        Raises:
            Exception: If a room insertion fails.
        """
        query = "INSERT INTO rooms (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING"
        for room in rooms:
            try:
                self.db.execute_query(query, (room["id"], room["name"]))
            except Exception as e:
                logger.error(f"Failed to insert {room}: {e}")
                raise

    def _insert_students(self, students: List[Dict]) -> None:
        """
        Insert student records into the database.

        Args:
            students (List[Dict]): List of student dictionaries containing 'id', 'name', 'room', 'sex', and 'birthday'.

        Raises:
            Exception: If a student insertion fails.
        """
        query = "INSERT INTO students (id, name, room_id, sex, birthday) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING"
        for student in students:
            try:
                self.db.execute_query(query, (
                    student["id"],
                    student["name"],
                    student["room"],
                    student["sex"],
                    student["birthday"]
                ))
            except Exception as e:
                logger.error(f"Failed to insert {student}: {e}")
                raise
