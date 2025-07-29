import asyncio
import json
import subprocess
import os
from typing import Optional, Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class FireboltMCPClient:
    """Client for interacting with Firebolt MCP Server"""
    
    def __init__(self, client_id: Optional[str] = None, client_secret: Optional[str] = None):
        self.client_id = client_id or os.getenv('FIREBOLT_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('FIREBOLT_CLIENT_SECRET')
        self.process = None
        self.connected = False
        
    async def connect(self) -> bool:
        """Connect to the Firebolt MCP server"""
        if not self.client_id or not self.client_secret:
            logger.warning("Firebolt credentials not provided. MCP features will be disabled.")
            return False
            
        try:
            # Start the MCP server process
            env = os.environ.copy()
            env['FIREBOLT_MCP_CLIENT_ID'] = self.client_id
            env['FIREBOLT_MCP_CLIENT_SECRET'] = self.client_secret
            
            # Try Docker first, fallback to binary
            cmd = [
                'docker', 'run', '-i', '--rm',
                '-e', 'FIREBOLT_MCP_CLIENT_ID',
                '-e', 'FIREBOLT_MCP_CLIENT_SECRET',
                'ghcr.io/firebolt-db/mcp-server:0.4.0'
            ]
            
            self.process = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env
            )
            
            self.connected = True
            logger.info("Connected to Firebolt MCP Server")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Firebolt MCP Server: {e}")
            return False
    
    async def get_firebolt_docs(self, topic: Optional[str] = None) -> Optional[str]:
        """Get Firebolt documentation using MCP server"""
        if not self.connected:
            return None
            
        try:
            request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "firebolt_docs",
                    "arguments": {"articles": [topic] if topic else []}
                }
            }
            
            return await self._send_request(request)
            
        except Exception as e:
            logger.error(f"Failed to get Firebolt docs: {e}")
            return None
    
    async def validate_query(self, account: str, database: str, engine: str, query: str) -> Optional[Dict]:
        """Validate a query using the Firebolt MCP server"""
        if not self.connected:
            return None
            
        try:
            request = {
                "jsonrpc": "2.0", 
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "firebolt_query",
                    "arguments": {
                        "account": account,
                        "database": database, 
                        "engine": engine,
                        "query": f"EXPLAIN {query}"  # Use EXPLAIN to validate without executing
                    }
                }
            }
            
            return await self._send_request(request)
            
        except Exception as e:
            logger.error(f"Failed to validate query: {e}")
            return None
    
    async def get_expert_conversion_prompt(self) -> Optional[str]:
        """Get the expert conversion prompt from MCP server"""
        if not self.connected:
            return None
            
        try:
            request = {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "prompts/get",
                "params": {
                    "name": "firebolt-expert"
                }
            }
            
            response = await self._send_request(request)
            if response and "result" in response:
                return response["result"].get("messages", [{}])[0].get("content", {}).get("text")
            
        except Exception as e:
            logger.error(f"Failed to get expert prompt: {e}")
            return None
    
    async def get_expert_correction(self, query: str, error_message: str) -> str:
        """
        Get expert correction suggestions from the MCP server for a failed query
        
        Args:
            query: The SQL query that failed
            error_message: The error message from Firebolt
            
        Returns:
            str: Suggested correction or guidance
        """
        try:
            if not self.connected:
                await self.connect()
            
            # Use the MCP server to get expert correction
            prompt = f"""
            This PostgreSQL query failed when converted to Firebolt:
            
            Query: {query}
            Error: {error_message}
            
            Please provide a corrected Firebolt-compatible version of this query.
            Focus on:
            1. JSON function syntax corrections
            2. Firebolt-specific function replacements  
            3. Data type casting fixes
            4. Engine-specific requirements
            
            Return only the corrected SQL query.
            """
            
            # Call the expert prompt tool
            result = await self._send_request({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "expert_prompt",
                    "arguments": {"prompt": prompt}
                }
            })
            
            if result and result.get("result") and result["result"].get("content"):
                if isinstance(result["result"]["content"], list) and len(result["result"]["content"]) > 0:
                    return result["result"]["content"][0].get("text", "").strip()
                elif isinstance(result["result"]["content"], dict) and result["result"]["content"].get("text"):
                    return result["result"]["content"]["text"].strip()
                else:
                    return str(result["result"]["content"]).strip()
            
            return "No correction available from MCP server"
            
        except Exception as e:
            logger.error(f"MCP expert correction failed: {str(e)}")
            return f"MCP correction failed: {str(e)}"
    
    async def _send_request(self, request: Dict) -> Optional[Dict]:
        """Send a request to the MCP server"""
        if not self.process or not self.process.stdin:
            return None
            
        try:
            # Send request
            request_json = json.dumps(request) + "\n"
            self.process.stdin.write(request_json.encode())
            await self.process.stdin.drain()
            
            # Read response
            response_line = await self.process.stdout.readline()
            if response_line:
                return json.loads(response_line.decode().strip())
                
        except Exception as e:
            logger.error(f"MCP request failed: {e}")
            return None
    
    async def disconnect(self):
        """Disconnect from the MCP server"""
        if self.process:
            try:
                self.process.terminate()
                await self.process.wait()
            except:
                pass
            self.process = None
        self.connected = False
        logger.info("Disconnected from Firebolt MCP Server")

# Singleton instance
_mcp_client = None

def get_mcp_client() -> FireboltMCPClient:
    """Get singleton MCP client instance"""
    global _mcp_client
    if _mcp_client is None:
        _mcp_client = FireboltMCPClient()
    return _mcp_client 