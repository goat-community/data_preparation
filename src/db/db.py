"""This module contains all classes and functions for database interactions."""
# Code based on
# https://github.com/hackersandslackers/psycopg2-tutorial/blob/master/psycopg2_tutorial/db.py
import logging as LOGGER
from multiprocessing import connection
import psycopg2
import sys,os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,parentdir)

from sqlalchemy import create_engine
from db.config import DATABASE, DATABASE_RD

user,password,host,port,dbname = DATABASE.values()
user_rd,password_rd,host_rd,port_rd,dbname_rd = DATABASE_RD.values()

class Database:
    """PostgreSQL Database class."""

    def __init__(self):
        self.conn = None
        try:
            connection_string = " ".join(("{}={}".format(*i) for i in DATABASE.items()))
            self.conn = psycopg2.connect(connection_string)
        except psycopg2.DatabaseError as e:
            LOGGER.error(e)
            raise e
        finally:
            LOGGER.getLogger().setLevel(LOGGER.INFO)   # To show logging.info in the console
            LOGGER.info('Connection opened successfully.')
        
    def connect_geopandas(self):
        if self.conn is None:
            try:
                # connection object
                self.conn = psycopg2.connect(database=dbname, user=user, password=password, host=host, port=port)     
            except Exception as e:
                LOGGER.error(e)
                raise e
            finally:
                LOGGER.getLogger().setLevel(LOGGER.INFO)   # To show logging.info in the console
                LOGGER.info('Connection opened successfully.')
        return self.conn

    def connect(self):
        """Connect to a Postgres database."""
        # if self.conn is None:
        try:
            connection_string = " ".join(("{}={}".format(*i) for i in DATABASE.items()))
            self.conn = psycopg2.connect(connection_string)
        except psycopg2.DatabaseError as e:
            LOGGER.error(e)
            raise e
        finally:
            LOGGER.getLogger().setLevel(LOGGER.INFO)   # To show logging.info in the console
            LOGGER.info('Connection opened successfully.')
        return self.conn
      
    def connect_rd(self):
        """Connect to a Postgres database."""
        if self.conn is None:
            try:
                connection_string = " ".join(("{}={}".format(*i) for i in DATABASE_RD.items()))
                self.conn = psycopg2.connect(connection_string)
            except psycopg2.DatabaseError as e:
                LOGGER.error(e)
                raise e
            finally:
                LOGGER.info('Connection opened successfully.')
        return self.conn
      
    def connect_sqlalchemy(self):
        print ("Connect to a Postgres database with engine")
        
        # if self.conn is None :    # check if it is connected with engine
        try:
            self.conn = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}", pool_pre_ping=True)
        except Exception as e:
            LOGGER.error(e)
            raise e
        finally:
            LOGGER.info('Connection opened successfully.')
        return self.conn
        
    def connect_rd_sqlalchemy(self):
        """Connect to a Postgres database with engine"""
        if self.conn is None:
            try:
                self.conn = create_engine(f"postgresql://{user_rd}:{password_rd}@{host_rd}:{port_rd}/{dbname_rd}")
            except Exception as e:
                LOGGER.error(e)
                raise e
            finally:
                LOGGER.info('Connection opened successfully.')
        return self.conn
        
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
    
    def select_rd(self, query, params=None):
        """Run a SQL query to select rows from table."""
        self.connect_rd()
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

    def perform_rd(self, query, params=None):
        """Run a SQL query that does not return anything"""
        self.connect_rd()
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

    def cursor_rd(self):
        """This will return the query as string for testing"""
        self.connect_rd()
        self.conn.cursor()
        return self.conn.cursor()
