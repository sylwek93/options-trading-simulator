import sqlite3
import pandas as pd
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
        JOIN gamma_levels g ON m.date_time = g.date_time
        WHERE m.date_time BETWEEN ? AND ?
        AND time(m.date_time) BETWEEN ? AND ?
        {}
        ORDER BY m.date_time
        """
    
    def execute_query(self, date_range, time_range, additional_conditions=""):
        try:
            if additional_conditions and not additional_conditions.strip():
                additional_conditions = ""
            
            if additional_conditions:
                additional_conditions = f"AND {additional_conditions}"
            
            query = self.base_query.format(additional_conditions)
            params = [date_range[0], date_range[1], time_range[0], time_range[1]]
            results = self.db_manager.execute_query(query, params)
            
            df = pd.DataFrame(results, columns=['date_time', 'spx_price'])
            return df
        except Exception as e:
            raise Exception(f"Query execution failed: {e}")


class QueryOptionChain:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.base_query = """
        SELECT time, strike, bid, ask
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
            
            df = pd.DataFrame(results, columns=['time', 'strike', 'bid', 'ask'])
            return df
        except Exception as e:
            raise Exception(f"Query execution failed: {e}")


db_manager = DatabaseManager()
conditions_query = QueryConditions(db_manager)
option_chain_query = QueryOptionChain(db_manager)


def query_with_conditions(date_range, time_range, additional_conditions=""):
    return conditions_query.execute_query(date_range, time_range, additional_conditions)


def query_option_chain(date, time, right, strike):
    return option_chain_query.execute_query(date, time, right, strike)