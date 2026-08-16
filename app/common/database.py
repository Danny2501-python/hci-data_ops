"""
Database Utilities
Shared database helper classes for PostgreSQL and other database operations
"""

import psycopg2
from typing import Optional, Tuple, List, Any
from loguru import logger


class PostgreSQLHelper:
    """
    Helper class for PostgreSQL operations with connection pooling and error handling.
    
    Features:
    - Automatic connection management with keepalive
    - Transaction support with automatic rollback on errors
    - Convenient query execution methods
    - Proper resource cleanup
    
    Usage:
        db = PostgreSQLHelper(connection_uri)
        
        # Execute a query
        db.execute("INSERT INTO table (col) VALUES (%s)", ("value",))
        
        # Fetch results
        result = db.fetchone("SELECT * FROM table WHERE id = %s", (1,))
        results = db.fetchall("SELECT * FROM table")
        
        # Close connection
        db.close()
        
    Or use as context manager:
        with PostgreSQLHelper(connection_uri) as db:
            results = db.fetchall("SELECT * FROM table")
    """
    
    def __init__(self, connection_uri: str, autocommit: bool = False):
        """
        Initialize PostgreSQL connection.
        
        Args:
            connection_uri: PostgreSQL connection string (e.g., postgresql://user:pass@host:port/db)
            autocommit: If True, automatically commit after each query. Default False.
        """
        self.connection_uri = connection_uri
        self.conn = psycopg2.connect(
            connection_uri,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        self.conn.autocommit = autocommit
        logger.debug(f"PostgreSQL connection established (autocommit={autocommit})")
    
    def execute(self, query: str, params: Optional[Tuple] = None) -> Any:
        """
        Execute a query with automatic commit/rollback.
        
        Args:
            query: SQL query string (use %s for parameters)
            params: Optional tuple of parameters for the query
            
        Returns:
            Cursor object (can be used to get lastrowid, rowcount, etc.)
            
        Raises:
            Exception: If query execution fails (automatically rolls back)
            
        Example:
            db.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("John", "john@example.com"))
            db.execute("UPDATE users SET active = TRUE WHERE id = %s", (123,))
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params)
            if not self.conn.autocommit:
                self.conn.commit()
            return cursor
        except Exception as e:
            if not self.conn.autocommit:
                self.conn.rollback()
            logger.error(f"Query execution failed: {e}")
            logger.debug(f"Failed query: {query}")
            logger.debug(f"Parameters: {params}")
            raise e
        finally:
            cursor.close()
    
    def fetchone(self, query: str, params: Optional[Tuple] = None) -> Optional[Tuple]:
        """
        Execute a query and fetch one result.
        
        Args:
            query: SQL query string
            params: Optional tuple of parameters
            
        Returns:
            Single row as tuple, or None if no results
            
        Example:
            user = db.fetchone("SELECT * FROM users WHERE id = %s", (123,))
            if user:
                print(f"User: {user[0]}, {user[1]}")
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchone()
        except Exception as e:
            if not self.conn.autocommit:
                self.conn.rollback()
            logger.error(f"Fetchone failed: {e}")
            logger.debug(f"Query: {query}, Params: {params}")
            raise e
        finally:
            cursor.close()
    
    def fetchall(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """
        Execute a query and fetch all results.
        
        Args:
            query: SQL query string
            params: Optional tuple of parameters
            
        Returns:
            List of rows (each row is a tuple)
            
        Example:
            users = db.fetchall("SELECT * FROM users WHERE active = %s", (True,))
            for user in users:
                print(f"User: {user[0]}, {user[1]}")
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        except Exception as e:
            if not self.conn.autocommit:
                self.conn.rollback()
            logger.error(f"Fetchall failed: {e}")
            logger.debug(f"Query: {query}, Params: {params}")
            raise e
        finally:
            cursor.close()
    
    def execute_many(self, query: str, params_list: List[Tuple]) -> None:
        """
        Execute the same query multiple times with different parameters (batch insert/update).
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
            
        Example:
            db.execute_many(
                "INSERT INTO users (name, email) VALUES (%s, %s)",
                [("John", "john@example.com"), ("Jane", "jane@example.com")]
            )
        """
        cursor = self.conn.cursor()
        try:
            cursor.executemany(query, params_list)
            if not self.conn.autocommit:
                self.conn.commit()
            logger.debug(f"Batch executed {len(params_list)} queries")
        except Exception as e:
            if not self.conn.autocommit:
                self.conn.rollback()
            logger.error(f"Batch execution failed: {e}")
            raise e
        finally:
            cursor.close()
    
    def close(self):
        """
        Close the database connection.
        
        Always call this when done, or use the context manager pattern.
        """
        if self.conn:
            self.conn.close()
            logger.debug("PostgreSQL connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - automatically closes connection."""
        self.close()
        return False  # Don't suppress exceptions
    
    def begin_transaction(self):
        """
        Explicitly begin a transaction (only needed if autocommit=True).
        """
        if self.conn.autocommit:
            self.conn.autocommit = False
            logger.debug("Transaction started")
    
    def commit(self):
        """
        Explicitly commit the current transaction.
        """
        self.conn.commit()
        logger.debug("Transaction committed")
    
    def rollback(self):
        """
        Explicitly rollback the current transaction.
        """
        self.conn.rollback()
        logger.debug("Transaction rolled back")


class PostgreSQLConnectionPool:
    """
    Simple connection pool for PostgreSQL (for future use).
    
    TODO: Implement connection pooling using psycopg2.pool.SimpleConnectionPool
    """
    pass
