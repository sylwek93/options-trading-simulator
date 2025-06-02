import sqlite3
import polars as pl
from contextlib import contextmanager
from config import DB_PATH


class DatabaseManager:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
    
    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_query(self, query, params=None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
    
    def get_table_info(self, table_name):
        query = f"PRAGMA table_info({table_name})"
        return self.execute_query(query)


class QueryConditions:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.base_query = """
        SELECT DISTINCT m.date_time, m.spx_price
        FROM metrics m
        LEFT JOIN gamma_levels g ON m.date_time = g.date_time
        WHERE m.date_time BETWEEN ? AND ?
        AND time(m.date_time) BETWEEN ? AND ?
        {}
        ORDER BY m.date_time
        """
    
    def execute_query(self, start_date, end_date, start_time, end_time, additional_conditions=""):
        try:
            if additional_conditions and not additional_conditions.strip():
                additional_conditions = ""
            
            if additional_conditions:
                additional_conditions = f"AND {additional_conditions}"
            
            query = self.base_query.format(additional_conditions)
            params = [start_date, end_date, start_time, end_time]
            results = self.db_manager.execute_query(query, params)
            
            df = pl.DataFrame({
                'date_time': [row[0] for row in results],
                'spx_price': [row[1] for row in results]
            })
            return df
        except Exception as e:
            raise Exception(f"Query execution failed: {e}")


class QueryOptionChain:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.base_query = """
        SELECT time, strike, bid, ask, spx_price
        FROM option_chain
        WHERE date = ?
        AND time >= ?
        AND right = ?
        AND strike = ?
        ORDER BY time
        """
    
    def execute_query(self, date, time, right, strike):
        try:
            params = [date, time, right, strike]
            results = self.db_manager.execute_query(self.base_query, params)
            
            df = pl.DataFrame({
                'time': [row[0] for row in results],
                'strike': [row[1] for row in results],
                'bid': [row[2] for row in results],
                'ask': [row[3] for row in results],
                'spx_price': [row[4] for row in results]
            })
            return df
        except Exception as e:
            raise Exception(f"Query execution failed: {e}")


db_manager = DatabaseManager()
conditions_query = QueryConditions(db_manager)
option_chain_query = QueryOptionChain(db_manager)


def query_with_conditions(start_date, end_date, start_time, end_time, additional_conditions=""):
    return conditions_query.execute_query(start_date, end_date, start_time, end_time, additional_conditions)


def query_option_chain(date, time, right, strike):
    return option_chain_query.execute_query(date, time, right, strike)