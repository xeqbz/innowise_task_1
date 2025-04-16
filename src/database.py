import configparser
from typing import Optional, Tuple
import logging
import psycopg2
from psycopg2 import Error

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Database:
    """
    A class to manage PostgreSQL database operations using psycopg2.

    Responsibilities:
    - Load database configuration from an .ini file
    - Connect and disconnect from the database
    - Execute SQL queries and return results
    """
    def __init__(self, config_path: str = 'config/config.ini'):
        """
        Initialize the Database instance.

        Args:
            config_path (str): Path to the database configuration file. Defaults to 'config/config.ini'.
        """
        self.config = self.load_config(config_path)
        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor = None

    @staticmethod
    def load_config(config_path: str = 'config/config.ini') -> configparser.ConfigParser:
        """
        Load database configuration from an INI file.

        Args:
            config_path (str): Path to the configuration file.

        Returns:
            configparser.ConfigParser: Parsed configuration object.
        """
        config = configparser.ConfigParser()
        config.read(config_path)
        return config

    def connect(self) -> None:
        """
        Establish a connection to the PostgreSQL database using the loaded configuration.

        Raises:
            psycopg2.Error: If the connection fails.
        """
        try:
            self.connection = psycopg2.connect(
                host = self.config['DATABASE']['host'],
                user = self.config['DATABASE']['user'],
                password = self.config['DATABASE']['password'],
                database = self.config['DATABASE']['database'],
                port = self.config['DATABASE']['port']
            )
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to PostgreSQL")
        except Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    def disconnect(self) -> None:
        """
        Close the database connection and cursor if they exist.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("Successfully disconnected from PostgreSQL")

    def execute_query(self, query: str, params: Tuple=()) -> list:
        """
        Execute a SQL query with optional parameters.

        Args:
            query (str): SQL query to be executed.
            params (Tuple, optional): Parameters to use with the query. Defaults to empty tuple.

        Returns:
            list: Query result as a list of tuples, or an empty list for non-select queries.

        Raises:
            psycopg2.Error: If query execution fails.
        """
        try:
            self.cursor.execute(query, params)
            if self.cursor.description:
                return self.cursor.fetchall()
            return []
        except Error as e:
            logger.error(f"Error executing query: {e}")
            raise
