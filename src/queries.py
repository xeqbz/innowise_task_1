import json
from src.database import Database
from typing import List, Dict, Any
import xml.etree.ElementTree as ET
from decimal import Decimal
from xml.dom import minidom


class QueryExecutor:
    """
        Executes database queries related to room and student information,
        and formats the results in JSON or XML format.
    """
    def __init__(self, db: 'Database'):
        """
        Initialize the QueryExecutor with a database instance.

        Args:
            db (Database): An instance of the Database class.
        """
        self.db = db

    def create_schema(self) -> None:
        """
        Creates the necessary database schema (tables) for rooms and students
        if they do not already exist. Also creates appropriate indexes.
        """
        schema_queries = [
            """
            CREATE TABLE IF NOT EXISTS rooms (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS students (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                room_id INTEGER NOT NULL,
                sex VARCHAR(1) NOT NULL,
                birthday TIMESTAMP NOT NULL,
                FOREIGN KEY (room_id) REFERENCES rooms(id)
            )
            """
        ]

        for query in schema_queries:
            self.db.execute_query(query)
        self.db.connection.commit()
        self._create_indexes()

    def _create_indexes(self) -> None:
        """
        Creates indexes on foreign keys and frequently filtered columns
        to optimize query performance.
        """
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_room_id ON students(room_id)",
            "CREATE INDEX IF NOT EXISTS idx_birthday ON students(birthday)",
            "CREATE INDEX IF NOT EXISTS idx_sex ON students(sex)"
        ]

        for idx in indexes:
            self.db.execute_query(idx)
        self.db.connection.commit()

    def get_room_student_count(self, output_format: str) -> str:
        """
        Retrieves the number of students in each room.

        Args:
            output_format (str): Output format ('json' or 'xml').

        Returns:
            str: Query result formatted as JSON or XML.
        """
        query = """
        SELECT r.id AS room_id, r.name, COUNT(s.id)::INTEGER AS student_count
        FROM rooms r
        LEFT JOIN students s on r.id = s.room_id
        GROUP BY r.id, r.name
        """

        results = self.db.execute_query(query)
        return self._format_output(results, output_format)

    def get_lowest_avg_age_rooms(self, output_format: str) -> str:
        """
        Retrieves the 5 rooms with the lowest average age of students.

        Args:
            output_format (str): Output format ('json' or 'xml').

        Returns:
            str: Query result formatted as JSON or XML.
        """
        query = """
        SELECT r.id AS room_id, r.name, 
            AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birthday)))::NUMERIC(10,2) AS avg_age
        FROM rooms r
        JOIN students s on r.id = s.room_id
        GROUP BY r.id, r.name
        ORDER BY avg_age
        LIMIT 5
        """

        results = self.db.execute_query(query)
        return self._format_output(results, output_format)

    def get_highest_age_diff_rooms(self, output_format: str) -> str:
        """
        Retrieves the 5 rooms with the greatest age difference among students.

        Args:
            output_format (str): Output format ('json' or 'xml').

        Returns:
            str: Query result formatted as JSON or XML.
        """
        query = """
        SELECT r.id AS room_id, r.name,
            (EXTRACT(YEAR FROM AGE(MAX(s.birthday), MIN(s.birthday))))::INTEGER as age_diff
        FROM rooms r
        JOIN students s on r.id = s.room_id
        GROUP BY r.id, r.name
        HAVING EXTRACT(YEAR FROM AGE(MAX(s.birthday), MIN(s.birthday))) > 0
        ORDER BY age_diff DESC
        LIMIT 5
        """

        results = self.db.execute_query(query)
        return self._format_output(results, output_format)

    def get_mixed_sex_rooms(self, output_format: str) -> str:
        """
        Retrieves all rooms that contain students of both sexes.

        Args:
            output_format (str): Output format ('json' or 'xml').

        Returns:
            str: Query result formatted as JSON or XML.
        """
        query = """
        SELECT r.id AS room_id, r.name
        FROM rooms r
        JOIN students s on r.id = s.room_id
        GROUP BY r.id, r.name
        HAVING COUNT(DISTINCT s.sex) > 1
        """

        results = self.db.execute_query(query)
        return self._format_output(results, output_format)

    def _format_output(self, data: List[Dict[str, Any]], output_format: str) -> str:
        """
        Format the raw query result into JSON or XML.

        Args:
            data (List[Dict[str, Any]]): Raw data from database query.
            output_format (str): Desired format ('json' or 'xml').

        Returns:
            str: Formatted string in specified format.
        """
        formatted_data = [
            {
                key.name: float(value) if isinstance(value, Decimal) else value
                for key, value in zip(self.db.cursor.description, row)
            }
            for row in data
        ]
        if output_format.lower() == "json":
            return json.dumps(formatted_data, indent=2)
        else:
            root = ET.Element("results")
            for item in formatted_data:
                record = ET.SubElement(root, "record")
                for key, value in item.items():
                    field = ET.SubElement(record, key)
                    field.text = str(value)
            rough_string = ET.tostring(root, encoding="unicode", method="xml")
            parsed = minidom.parseString(rough_string)
            return parsed.toprettyxml(indent="  ", encoding=None, newl='\n')
