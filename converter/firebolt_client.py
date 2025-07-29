"""
Firebolt client for PostgreSQL to Firebolt converter.
Uses the official Firebolt Python SDK for reliable connections.
"""

import logging
import time
import asyncio
import concurrent.futures
from typing import Tuple, Dict, Any, Optional
from firebolt.db import connect
from firebolt.client.auth import ClientCredentials

logger = logging.getLogger(__name__)

class FireboltClient:
    """
    Firebolt client using the official Firebolt Python SDK
    """
    
    def __init__(self, client_id: str = None, client_secret: str = None, account: str = None, database: str = None, engine: str = None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.account = account
        self.database = database
        self.engine = engine
        self.connection = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        
    async def connect(self, client_id: str = None, client_secret: str = None, account: str = None, database: str = None, engine: str = None) -> bool:
        """
        Connect to Firebolt with provided credentials
        
        Args:
            client_id: Firebolt client ID
            client_secret: Firebolt client secret  
            account: Firebolt account name
            database: Firebolt database name
            engine: Firebolt engine name (optional)
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        # Update instance variables with provided credentials
        if client_id:
            self.client_id = client_id
        if client_secret:
            self.client_secret = client_secret
        if account:
            self.account = account
        if database:
            self.database = database
        if engine:
            self.engine = engine
            
        # Validate we have all required credentials
        if not all([self.client_id, self.client_secret, self.account, self.database]):
            logger.error("Missing required Firebolt credentials")
            return False
            
        # Attempt to authenticate
        return await self.authenticate()
    
    async def authenticate(self) -> bool:
        """
        Establish connection to Firebolt using the official SDK
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            logger.info(f"Attempting authentication for account: {self.account}")
            
            def _connect():
                auth = ClientCredentials(
                    client_id=self.client_id,
                    client_secret=self.client_secret
                )
                
                return connect(
                    auth=auth,
                    account_name=self.account,
                    database=self.database,
                    engine_name=self.engine
                )
            
            # Connect using thread executor to avoid blocking
            self.connection = await asyncio.get_event_loop().run_in_executor(
                self.executor, _connect
            )
            
            logger.info("Successfully authenticated with Firebolt")
            return True
            
        except Exception as e:
            logger.error(f"Firebolt authentication failed: {str(e)}")
            return False
    
    async def ensure_authenticated(self) -> bool:
        """
        Ensure we have a valid connection
        
        Returns:
            bool: True if authenticated, False otherwise
        """
        if not self.connection:
            return await self.authenticate()
        return True
    
    async def execute_query(self, sql: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute a SQL query against Firebolt database using the official SDK
        
        Returns:
            Tuple[bool, Dict]: (success, result_data)
            - success: True if query executed successfully
            - result_data: Contains either results or error information
        """
        try:
            # Ensure we're authenticated
            if not await self.ensure_authenticated():
                return False, {"error": "Authentication failed", "error_type": "auth_error"}
            
            logger.info(f"Executing query against database: {self.database}, engine: {self.engine}")
            logger.info(f"Query: {sql}")
            
            def _execute_query():
                with self.connection.cursor() as cursor:
                    cursor.execute(sql)
                    
                    # Fetch results
                    results = cursor.fetchall()
                    
                    # Get column names
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    
                    # Convert to list of dictionaries
                    data = [dict(zip(columns, row)) for row in results]
                    
                    return data
            
            # Execute query using thread executor
            data = await asyncio.get_event_loop().run_in_executor(
                self.executor, _execute_query
            )
            
            logger.info("Query executed successfully")
            return True, {
                "success": True,
                "data": data,
                "rows_affected": len(data),
                "meta": {"columns": data[0].keys() if data else []}
            }
                
        except Exception as e:
            error_message = str(e)
            logger.error(f"Query execution failed: {error_message}")
            
            # Parse common Firebolt errors
            if "does not exist" in error_message.lower():
                if "database" in error_message.lower():
                    error_message = f"Database '{self.database}' not found. Please check the database name in your Firebolt console."
                elif "engine" in error_message.lower():
                    error_message = f"Engine '{self.engine}' not found. Please check the engine name or try leaving it empty."
                elif "table" in error_message.lower():
                    error_message = f"Table referenced in query does not exist. Please check your table names."
            
            return False, {
                "success": False,
                "error": error_message,
                "error_type": "execution_error"
            }
    
    async def test_connection(self) -> Tuple[bool, str]:
        """
        Test the connection by running a simple query
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            success, result = await self.execute_query("SELECT 1 as test")
            
            if success:
                return True, "Connection successful! Firebolt is ready for query conversion."
            else:
                return False, f"Connection failed: {result.get('error', 'Unknown error')}"
                
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"
    
    async def close(self):
        """Close database connection"""
        if self.connection:
            def _close():
                self.connection.close()
            
            await asyncio.get_event_loop().run_in_executor(
                self.executor, _close
            )
            
            self.connection = None
        
        self.executor.shutdown(wait=True)
        logger.info("Firebolt connection closed")
    
    def get_connection_info(self) -> Dict[str, str]:
        """Get connection information for display"""
        return {
            "account": self.account,
            "database": self.database,
            "engine": self.engine or "default",
            "status": "authenticated" if self.connection else "not authenticated"
        } 