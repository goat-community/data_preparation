"""This module contains all classes and functions for database interactions."""
# Code based on
# https://github.com/hackersandslackers/psycopg2-tutorial/blob/master/psycopg2_tutorial/db.py
import logging as LOGGER
import psycopg2
from sqlalchemy import create_engine

class Database:
    """PostgreSQL Database class."""
    def __init__(self, db_config: dict):
        self.db_config = db_config
        try:
            self.conn = psycopg2.connect(self.db_config)
        except psycopg2.DatabaseError as e:
            LOGGER.error(e)
            raise e
        finally:
            LOGGER.getLogger().setLevel(LOGGER.INFO)   # To show logging.info in the console
            LOGGER.info('Connection opened successfully.')
    
    def return_sqlalchemy_engine(self):  
        """This will create SQLAlchemy engine for the database"""
        return create_engine(self.db_config, future=False)
                
    def select(self, query, params=None):
        """Run a SQL query to select rows from table."""
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
        with self.conn.cursor() as cur:
            if params is None:
                cur.execute(query)
            else:
                cur.execute(query, params)
        self.conn.commit()
        cur.close()

    def mogrify_query(self, query, params=None):
        """This will return the query as string for testing"""
        with self.conn.cursor() as cur:
            if params is None:
                result = cur.mogrify(query)
            else:
                result = cur.mogrify(query, params)
        cur.close()
        return result

    def cursor(self):
        """This will return the query as string for testing"""
        return self.conn.cursor()
