import psycopg2
from psycopg2.extras import RealDictCursor

from core.config import DB_CONFIG


class Database:
    """
    PostgreSQL database manager.

    Handles:
    - Connection creation
    - Transactions
    - Commit
    - Rollback
    - Query execution
    - Fetch operations
    """


    def __init__(self):
        self.connection = None
        self.cursor = None


    def __enter__(self):
        self.connect()
        return self


    def __exit__(self, exc_type, exc_value, traceback):

        if exc_type:
            self.rollback()
        else:
            self.commit()

        self.close()


    def connect(self):

        self.connection = psycopg2.connect(
            **DB_CONFIG
        )

        self.cursor = self.connection.cursor(
            cursor_factory=RealDictCursor
        )


    def execute(
        self,
        query,
        params=None
    ):
        """
        Execute INSERT/UPDATE/DELETE queries.
        """

        self.cursor.execute(
            query,
            params
        )


    def fetch_one(
        self,
        query,
        params=None
    ):
        """
        Return single row.
        """

        self.cursor.execute(
            query,
            params
        )

        return self.cursor.fetchone()


    def fetch_all(
        self,
        query,
        params=None
    ):
        """
        Return multiple rows.
        """

        self.cursor.execute(
            query,
            params
        )

        return self.cursor.fetchall()


    def commit(self):

        if self.connection:
            self.connection.commit()


    def rollback(self):

        if self.connection:
            self.connection.rollback()


    def close(self):

        if self.cursor:
            self.cursor.close()

        if self.connection:
            self.connection.close()