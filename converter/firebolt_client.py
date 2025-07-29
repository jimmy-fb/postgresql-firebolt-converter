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
        
        # Store original credentials for restoration if needed
        self._original_credentials = {
            'client_id': client_id,
            'client_secret': client_secret,
            'account': account,
            'database': database,
            'engine': engine
        }
        
    def _restore_credentials(self):
        """Restore credentials if they were lost"""
        if not all([self.client_id, self.client_secret, self.account, self.database]):
            logger.warning("Credentials lost, restoring from backup...")
            self.client_id = self._original_credentials['client_id']
            self.client_secret = self._original_credentials['client_secret']
            self.account = self._original_credentials['account']
            self.database = self._original_credentials['database']
            self.engine = self._original_credentials['engine']
            
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
            
        # Update backup credentials
        self._original_credentials.update({
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'account': self.account,
            'database': self.database,
            'engine': self.engine
        })
            
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
            # Try to restore credentials if they were lost
            self._restore_credentials()
            
            logger.info(f"Attempting authentication for account: {self.account}")
            logger.info(f"Database: {self.database}, Engine: {self.engine}")
            logger.info(f"Client ID present: {bool(self.client_id)}")
            logger.info(f"Client Secret present: {bool(self.client_secret)}")
            
            # Validate all required parameters are present
            if not self.client_id:
                logger.error("Missing client_id for authentication")
                return False
            if not self.client_secret:
                logger.error("Missing client_secret for authentication")
                return False
            if not self.account:
                logger.error("Missing account for authentication")
                return False
            if not self.database:
                logger.error("Missing database for authentication")
                return False
            
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
        # Always try to restore credentials first
        self._restore_credentials()
        
        if not self.connection:
            logger.info("No active connection, attempting to authenticate...")
            return await self.authenticate()
        
        # Test the existing connection
        try:
            def _test_connection():
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                return True
            
            await asyncio.get_event_loop().run_in_executor(
                self.executor, _test_connection
            )
            logger.info("Existing connection is valid")
            return True
            
        except Exception as e:
            logger.warning(f"Existing connection failed test: {str(e)}, re-authenticating...")
            self.connection = None
            return await self.authenticate()
    
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
                # If authentication failed, check if we still have credentials for better error message
                if not all([self.client_id, self.client_secret, self.account, self.database]):
                    error_msg = f"Authentication failed - Missing credentials: client_id={bool(self.client_id)}, client_secret={bool(self.client_secret)}, account={bool(self.account)}, database={bool(self.database)}"
                    logger.error(error_msg)
                    return False, {"error": error_msg, "error_type": "auth_error"}
                else:
                    return False, {"error": "Authentication failed", "error_type": "auth_error"}
            
            # Execute query using thread executor to avoid blocking
            def _execute_query():
                cursor = self.connection.cursor()
                cursor.execute(sql)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                return results, columns
            
            results, columns = await asyncio.get_event_loop().run_in_executor(
                self.executor, _execute_query
            )
            
            logger.info(f"Query executed successfully, returned {len(results)} rows")
            
            return True, {
                "results": results,
                "columns": columns,
                "row_count": len(results)
            }
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Query execution failed: {error_msg}")
            
            # Reset connection on error to force re-authentication on next query
            self.connection = None
            
            return False, {
                "error": error_msg,
                "error_type": self._categorize_error(error_msg)
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