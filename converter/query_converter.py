import re
import json
import sqlparse
from typing import Dict, List, Tuple, Optional
import openai
import os
import asyncio
from .conversion_rules import ConversionRules
from .firebolt_mappings import FUNCTION_MAPPINGS, DATA_TYPE_MAPPINGS, JSON_OPERATORS
from .mcp_client import get_mcp_client

class PostgreSQLToFireboltConverter:
    """Main converter class for PostgreSQL to Firebolt queries"""
    
    def __init__(self, openai_api_key: Optional[str] = None, use_mcp: bool = False, mcp_client=None):
        """
        Initialize the PostgreSQL to Firebolt converter
        
        Args:
            openai_api_key: OpenAI API key for AI-powered conversions
            use_mcp: Whether to use MCP server for enhanced conversions
            mcp_client: MCP client instance for Firebolt integration
        """
        self.openai_api_key = openai_api_key or os.getenv('OPENAI_API_KEY')
        self.use_mcp = use_mcp
        self.mcp_client = mcp_client
        
        if self.openai_api_key:
            try:
                import openai
                openai.api_key = self.openai_api_key
                self.openai_available = True
            except ImportError:
                print("OpenAI package not installed. AI features will be disabled.")
                self.openai_available = False
        else:
            self.openai_available = False
            print("No OpenAI API key provided. Using rule-based conversion only.")
        
        # Initialize conversion patterns and mappings
        from .firebolt_mappings import FUNCTION_MAPPINGS, DATA_TYPE_MAPPINGS, JSON_OPERATORS
        from .conversion_rules import ConversionRules
        
        self.function_mappings = FUNCTION_MAPPINGS
        self.datatype_mappings = DATA_TYPE_MAPPINGS
        self.json_operators = JSON_OPERATORS
        self.rules = ConversionRules()
        self.mcp_initialized = False

    async def _ensure_mcp_connected(self):
        """Ensure MCP is connected when needed"""
        if self.use_mcp and self.mcp_client and not self.mcp_initialized:
            try:
                await self.mcp_client.connect()
                self.mcp_initialized = True
            except Exception as e:
                print(f"MCP initialization failed: {e}")
                self.use_mcp = False

    def convert(self, postgresql_query: str) -> dict:
        """Convert PostgreSQL query to Firebolt SQL - OpenAI ONLY"""
        
        # Clean up the query
        cleaned_query = postgresql_query.strip()
        warnings = []
        explanation_parts = []
        
        # ONLY try AI conversion - no fallback
        try:
            converted_sql = self._apply_ai_conversion(cleaned_query)
            explanation_parts.append("Used AI-powered conversion with OpenAI")
            
            return {
                'converted_sql': converted_sql,
                'warnings': warnings,
                'explanations': explanation_parts,
                'method_used': 'ai_powered'
            }
            
        except Exception as e:
            print(f"AI conversion failed: {str(e)}")
            
            # NO FALLBACK - OpenAI only approach
            error_msg = f"Conversion failed: {str(e)}. Please ensure OPENAI_API_KEY is set."
            warnings.append(error_msg)
            
            return {
                'converted_sql': cleaned_query,  # Return original
                'warnings': warnings,
                'explanations': ["❌ AI conversion failed - no fallback used"],
                'method_used': 'failed'
            }

    def _apply_ai_conversion(self, sql: str) -> str:
        """Apply AI-powered conversion using OpenAI - Simple and Direct"""
        try:
            import openai
            
            # Simple, direct prompt - let OpenAI handle everything
            prompt = f"""Convert this PostgreSQL query to be fully Firebolt compliant.

Fix ALL Firebolt compatibility issues including:
- EXTRACT functions (wrap expressions with CAST AS DATE/TIMESTAMP)  
- FILTER clauses (convert to CASE WHEN)
- PostgreSQL casting (:: to CAST())
- JSON operations - IMPORTANT: Use Firebolt JSON functions:
  * JSONExtract() does NOT exist in Firebolt
  * Use JSON_POINTER_EXTRACT_TEXT(json_column, '/path') for extracting JSON values
  * Use JSON_VALUE(JSON_POINTER_EXTRACT(json_column, '/path')) for complex extraction
  * JSON paths use / syntax (not $ syntax): '/key' not '$.key'
  * Replace PostgreSQL -> and ->> with Firebolt JSON functions
  * See: https://docs.firebolt.io/reference-sql/functions-reference/json
- Date/time functions
- Subquery syntax
- Any other Firebolt requirements

PostgreSQL Query:
{sql}

Return ONLY the corrected Firebolt SQL:"""
            
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a Firebolt SQL expert. Convert PostgreSQL queries to be fully Firebolt compliant. Return only the corrected SQL."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=4000,
                temperature=0.1
            )
            
            converted = response.choices[0].message.content.strip()
            
            # Clean up any markdown formatting
            if converted.startswith('```sql'):
                converted = converted[6:]
            elif converted.startswith('```'):
                converted = converted[3:]
            if converted.endswith('```'):
                converted = converted[:-3]
                
            return converted.strip()
            
        except Exception as e:
            raise Exception(f"OpenAI conversion error: {str(e)}")

    def _apply_rule_based_conversion(self, sql: str) -> str:
        """REMOVED - OpenAI-only approach"""
        # logger.warning("⚠️ Rule-based conversion disabled - OpenAI only") # This line was removed from the new_code
        return sql
    
    def _convert_json_operations(self, sql: str) -> str:
        """REMOVED - OpenAI-only approach"""
        return sql
    
    def _convert_filter_clauses(self, sql: str) -> str:
        """REMOVED - OpenAI-only approach"""
        return sql
    
    def _convert_data_types(self, sql: str) -> str:
        """REMOVED - OpenAI-only approach"""
        return sql
    
    def _convert_array_operations(self, query: str, result: Dict) -> str:
        """Convert PostgreSQL array operations to Firebolt equivalents"""
        
        # Convert array_agg to ARRAY_AGG
        pattern = r'\barray_agg\b'
        if re.search(pattern, query, re.IGNORECASE):
            query = re.sub(pattern, 'ARRAY_AGG', query, flags=re.IGNORECASE)
            result['explanations'].append("Converted array_agg to ARRAY_AGG")
        
        # Convert unnest to explode (if supported)
        pattern = r'\bunnest\s*\(\s*([^)]+)\s*\)'
        if re.search(pattern, query, re.IGNORECASE):
            result['warnings'].append("unnest function may need manual conversion to table-valued function in Firebolt")
        
        return query
    
    def _convert_datetime_functions(self, query: str, result: Dict) -> str:
        """Convert PostgreSQL datetime functions to Firebolt equivalents"""
        
        # Convert now() to CURRENT_TIMESTAMP
        pattern = r'\bnow\s*\(\s*\)'
        if re.search(pattern, query, re.IGNORECASE):
            query = re.sub(pattern, 'CURRENT_TIMESTAMP', query, flags=re.IGNORECASE)
            result['explanations'].append("Converted now() to CURRENT_TIMESTAMP")
        
        # Convert date_trunc to DATE_TRUNC (if similar)
        pattern = r'\bdate_trunc\s*\(\s*[\'"]([^\'"]+)[\'"],\s*([^)]+)\s*\)'
        def replace_date_trunc(match):
            interval, column = match.groups()
            result['explanations'].append(f"Converted date_trunc with {interval} interval")
            return f"DATE_TRUNC('{interval}', {column})"
        
        query = re.sub(pattern, replace_date_trunc, query, flags=re.IGNORECASE)
        
        return query
    
    def _convert_string_functions(self, query: str, result: Dict) -> str:
        """Convert PostgreSQL string functions to Firebolt equivalents"""
        
        # Convert position to POSITION (if different syntax)
        pattern = r'\bposition\s*\(\s*[\'"]([^\'"]+)[\'"]\s+in\s+([^)]+)\s*\)'
        def replace_position(match):
            search_str, column = match.groups()
            result['explanations'].append("Converted position function syntax")
            return f"POSITION('{search_str}' IN {column})"
        
        query = re.sub(pattern, replace_position, query, flags=re.IGNORECASE)
        
        return query
    
    def _convert_window_functions(self, query: str, result: Dict) -> str:
        """Convert PostgreSQL window functions to Firebolt equivalents"""
        
        # Most window functions should be similar, but add warnings for complex ones
        window_functions = ['row_number', 'rank', 'dense_rank', 'lead', 'lag', 'first_value', 'last_value']
        
        for func in window_functions:
            pattern = r'\b' + func + r'\s*\('
            if re.search(pattern, query, re.IGNORECASE):
                result['explanations'].append(f"Window function {func} should work similarly in Firebolt")
        
        return query 

    async def _get_direct_firebolt_equivalent(self, postgresql_query: str) -> str:
        """Ask MCP server directly: what is the Firebolt equivalent of this PostgreSQL query?"""
        try:
            # Ensure MCP is connected
            await self._ensure_mcp_connected()
            
            if not self.mcp_client or not self.mcp_client.connected:
                raise Exception("MCP client not available")
            
            # Ask MCP server directly for the Firebolt equivalent
            prompt = f"""What is the exact Firebolt SQL equivalent of this PostgreSQL query?

IMPORTANT: Only convert what actually needs to be converted for Firebolt compatibility. 
Do NOT convert regular column names to JSON functions unless they use JSON operators.

PostgreSQL Query:
{postgresql_query}

Return ONLY the equivalent Firebolt SQL, no explanation:"""

            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a Firebolt SQL expert. Convert only what needs to be converted. Do NOT add JSON functions to regular columns."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=2000,
                temperature=0.1
            )
            
            converted = response.choices[0].message.content.strip()
            
            # Clean up markdown
            if converted.startswith('```sql'):
                converted = converted[6:]
            elif converted.startswith('```'):
                converted = converted[3:]
            if converted.endswith('```'):
                converted = converted[:-3]
                
            return converted.strip()
            
        except Exception as e:
            raise Exception(f"Direct MCP query failed: {str(e)}") 