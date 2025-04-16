import json
import logging
from typing import List, Dict
from database import Database

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, db: 'Database'):
        self.db = db

    def load_data(self, rooms_file: str, students_file: str) -> None:
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
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
            raise

    def _insert_rooms(self, rooms: List[Dict]) -> None:
        query = "INSERT INTO rooms (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING"
        for room in rooms:
            try:
                self.db.execute_query(query, (room["id"], room["name"]))
            except Exception as e:
                logger.error(f"Failed to insert {room}: {e}")
                raise

    def _insert_students(self, students: List[Dict]) -> None:
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
