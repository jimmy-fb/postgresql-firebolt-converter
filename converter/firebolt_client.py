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
        # Only store non-None values
        self._original_credentials = {}
        if client_id:
            self._original_credentials['client_id'] = client_id
        if client_secret:
            self._original_credentials['client_secret'] = client_secret
        if account:
            self._original_credentials['account'] = account
        if database:
            self._original_credentials['database'] = database
        if engine:
            self._original_credentials['engine'] = engine
        
    def _restore_credentials(self):
        """Restore credentials if they were lost"""
        restored = False
        if not self.client_id and 'client_id' in self._original_credentials:
            self.client_id = self._original_credentials['client_id']
            restored = True
        if not self.client_secret and 'client_secret' in self._original_credentials:
            self.client_secret = self._original_credentials['client_secret']
            restored = True
        if not self.account and 'account' in self._original_credentials:
            self.account = self._original_credentials['account']
            restored = True
        if not self.database and 'database' in self._original_credentials:
            self.database = self._original_credentials['database']
            restored = True
        if not self.engine and 'engine' in self._original_credentials:
            self.engine = self._original_credentials['engine']
            restored = True
            
        if restored:
            logger.info("✅ Successfully restored credentials from backup")
            logger.info(f"   Client ID present: {bool(self.client_id)}")
            logger.info(f"   Account: {self.account}")
            logger.info(f"   Database: {self.database}")
        else:
            logger.warning("⚠️ Could not restore credentials - backup may be empty")
            
    def _debug_credential_state(self):
        """Debug method to log current credential state"""
        logger.info("🔍 Current credential state:")
        logger.info(f"   client_id: {'✅' if self.client_id else '❌'}")
        logger.info(f"   client_secret: {'✅' if self.client_secret else '❌'}")
        logger.info(f"   account: {'✅' if self.account else '❌'} ({self.account})")
        logger.info(f"   database: {'✅' if self.database else '❌'} ({self.database})")
        logger.info(f"   engine: {'✅' if self.engine else '❌'} ({self.engine})")
        logger.info(f"   backup keys: {list(self._original_credentials.keys())}")
             
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
            self._original_credentials['client_id'] = client_id
        if client_secret:
            self.client_secret = client_secret
            self._original_credentials['client_secret'] = client_secret
        if account:
            self.account = account
            self._original_credentials['account'] = account
        if database:
            self.database = database
            self._original_credentials['database'] = database
        if engine:
            self.engine = engine
            self._original_credentials['engine'] = engine
            
        logger.info(f"🔐 Credentials updated:")
        logger.info(f"   Client ID present: {bool(self.client_id)}")
        logger.info(f"   Account: {self.account}")
        logger.info(f"   Database: {self.database}")
        logger.info(f"   Engine: {self.engine}")
        logger.info(f"   Backup size: {len(self._original_credentials)} credentials stored")
            
        # Validate we have all required credentials
        if not all([self.client_id, self.client_secret, self.account, self.database]):
            missing = []
            if not self.client_id: missing.append("client_id")
            if not self.client_secret: missing.append("client_secret")
            if not self.account: missing.append("account")
            if not self.database: missing.append("database")
            logger.error(f"Missing required Firebolt credentials: {missing}")
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
        # Always try to restore credentials first if any are missing
        if not all([self.client_id, self.client_secret, self.account, self.database]):
            logger.warning("🔧 Some credentials missing, attempting restore...")
            logger.debug("Before restore:")
            self._debug_credential_state()
            self._restore_credentials()
            logger.debug("After restore:")
            self._debug_credential_state()
        
        if not self.connection:
            logger.info("No active connection, attempting to authenticate...")
            return await self.authenticate()
        
        # Test the existing connection with a simple query
        try:
            def _test_connection():
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                return True
            
            await asyncio.get_event_loop().run_in_executor(
                self.executor, _test_connection
            )
            logger.debug("Existing connection is valid")
            return True
            
        except Exception as e:
            logger.warning(f"Existing connection failed test: {str(e)}, re-authenticating...")
            self.connection = None
            # Restore credentials before re-authenticating
            logger.debug("Before restore after connection failure:")
            self._debug_credential_state()
            self._restore_credentials()
            logger.debug("After restore after connection failure:")
            self._debug_credential_state()
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