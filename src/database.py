import configparser
from typing import Optional, Tuple
import logging
import psycopg2
from psycopg2 import Error

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, config_path: str):
        self.config = self.load_config(config_path)
        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor = None

    @staticmethod
    def load_config(self, config_path: str = 'config/config.ini') -> configparser.ConfigParser:
        config = configparser.ConfigParser()
        config.read(config_path)
        return config

    def connect(self) -> None:
        try:
            self.connection = psycopg2.connect(
                host = self.config['database']['host'],
                user = self.config['database']['user'],
                password = self.config['database']['password'],
                database = self.config['database']['database'],
                port = self.config['database']['port']
            )
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to PostgreSQL")
        except Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    def disconnect(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("Successfully disconnected from PostgreSQL")

    def execute_query(self, query: str, params: Tuple=()) -> list:
        try:
            self.cursor.execute(query, params)
            if self.cursor.description:
                return self.cursor.fetchall()
            return []
        except Error as e:
            logger.error(f"Error executing query: {e}")
            raise