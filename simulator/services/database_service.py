from contextlib import contextmanager
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor
import psycopg2
import logging

logger = logging.getLogger(__name__)


class DatabaseService:
    """
    Generic PostgreSQL database service.

    Every generator/service should use this class instead of
    talking directly to psycopg2.
    """

    def __init__(
        self,
        host,
        port,
        database,
        user,
        password,
        min_connections=1,
        max_connections=10,
    ):

        self.pool = SimpleConnectionPool(
            min_connections,
            max_connections,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )

        logger.info("PostgreSQL connection pool initialized.")

    def close(self):
        self.pool.closeall()
        logger.info("Connection pool closed.")

    @contextmanager
    def connection(self):
        conn = self.pool.getconn()

        try:
            yield conn
            conn.commit()

        except Exception:
            conn.rollback()
            raise

        finally:
            self.pool.putconn(conn)

    def execute(self, sql, params=None):
        """
        Execute INSERT/UPDATE/DELETE or DDL.
        """

        with self.connection() as conn:

            with conn.cursor() as cursor:
                cursor.execute(sql, params)

    def fetch_one(self, sql, params=None):

        with self.connection() as conn:

            with conn.cursor(cursor_factory=RealDictCursor) as cursor:

                cursor.execute(sql, params)

                return cursor.fetchone()

    def fetch_all(self, sql, params=None):

        with self.connection() as conn:

            with conn.cursor(cursor_factory=RealDictCursor) as cursor:

                cursor.execute(sql, params)

                return cursor.fetchall()

    def insert(self, sql, params=None, returning=False):

        with self.connection() as conn:

            with conn.cursor(cursor_factory=RealDictCursor) as cursor:

                cursor.execute(sql, params)

                if returning:
                    return cursor.fetchone()

    def update(self, sql, params=None):

        with self.connection() as conn:

            with conn.cursor() as cursor:

                cursor.execute(sql, params)

                return cursor.rowcount

    def delete(self, sql, params=None):

        with self.connection() as conn:

            with conn.cursor() as cursor:

                cursor.execute(sql, params)

                return cursor.rowcount