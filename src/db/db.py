"""This module contains all classes and functions for database interactions."""
# Code based on
# https://github.com/hackersandslackers/psycopg2-tutorial/blob/master/psycopg2_tutorial/db.py
import logging as LOGGER
import psycopg2
from sqlalchemy import create_engine
from db.config import DATABASE

user,password,host,port,dbname = DATABASE.values()

class Database:
    """PostgreSQL Database class."""

    def __init__(self):
        self.conn = None

    def connect(self):
        """Connect to a Postgres database."""
        if self.conn is None:
            try:
                connection_string = " ".join(("{}={}".format(*i) for i in DATABASE.items()))
                self.conn = psycopg2.connect(connection_string)
            except psycopg2.DatabaseError as e:
                LOGGER.error(e)
                raise e
            finally:
                LOGGER.info('Connection opened successfully.')
        return self.conn
    def connect_sqlalchemy(self):
        """Connect to a Postgres database with engine"""
        if self.conn is None:
            try:
                self.conn = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
            except Exception as e:
                LOGGER.error(e)
                raise e
            finally:
                LOGGER.info('Connection opened successfully.')
        return self.conn
    def select(self, query, params=None):
        """Run a SQL query to select rows from table."""
        self.connect()
        with self.conn.cursor() as cur:
            if params is None:
                cur.execute(query)
            else:
                cur.execute(query, params)
            records = cur.fetchall()
        cur.close()
        return records

    def perform(self, query, params=None):
        """Run a SQL query that does not return anything"""
        self.connect()
        with self.conn.cursor() as cur:
            if params is None:
                cur.execute(query)
            else:
                cur.execute(query, params)
        self.conn.commit()
        cur.close()

    def mogrify_query(self, query, params=None):
        """This will return the query as string for testing"""
        self.connect()
        with self.conn.cursor() as cur:
            if params is None:
                result = cur.mogrify(query)
            else:
                result = cur.mogrify(query, params)
        cur.close()
        return result

    def fetch_one(self, query, params=None):
        """This will return the next row in the result set"""
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(query)
            if not cur:
                self.send_error(404, f"sql query failed: {(query)}")
                return None
            return cur.fetchone()[0]

    def cursor(self):
        """This will return the query as string for testing"""
        self.connect()
        self.conn.cursor()
        return self.conn.cursor()

#%%
