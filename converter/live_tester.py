"""
Live PostgreSQL to Firebolt Query Testing and Auto-Correction

This module orchestrates the complete flow:
1. Convert PostgreSQL query to Firebolt
2. Test against real Firebolt database
3. If errors occur, analyze and auto-correct using MCP
4. Repeat until query works or max attempts reached
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
from .query_converter import PostgreSQLToFireboltConverter
from .firebolt_client import FireboltClient
from .error_analyzer import FireboltErrorAnalyzer
from .mcp_client import FireboltMCPClient
import time
import re
import os

logger = logging.getLogger(__name__)

class LiveQueryTester:
    """Live testing and auto-correction for PostgreSQL to Firebolt conversion"""
    
    def __init__(self, firebolt_client, query_converter, error_analyzer=None, 
                 openai_api_key=None, use_mcp=False, mcp_client=None):
        self.firebolt_client = firebolt_client
        self.query_converter = query_converter  # Fix the attribute name
        self.error_analyzer = error_analyzer
        # Get OpenAI API key from multiple sources
        self.openai_api_key = openai_api_key or os.getenv('OPENAI_API_KEY')
        self.use_mcp = use_mcp  # Add this attribute
        self.mcp_client = mcp_client  # Add this attribute
        self.max_attempts = 3 # Default max attempts
        
        # Initialize OpenAI if key is available
        if self.openai_api_key:
            try:
                import openai
                openai.api_key = self.openai_api_key
                logger.info("✅ OpenAI initialized for error correction")
            except Exception as e:
                logger.warning(f"⚠️ OpenAI initialization failed: {e}")
        else:
            logger.warning("⚠️ No OpenAI API key found - corrections will be limited")
        
    async def test_and_fix_query(self, postgresql_query: str, max_correction_attempts: int = 10) -> Dict[str, Any]:
        """Test query against Firebolt and attempt to fix any errors with more attempts"""
        
        attempts = []
        converted_query = postgresql_query
        error_history = {}  # Track errors we've seen: {error_message: [attempt_numbers]}
        
        # First convert the PostgreSQL query to Firebolt
        try:
            conversion_result = self.query_converter.convert(postgresql_query)
            converted_query = conversion_result.get('converted_sql', postgresql_query)
            
            logger.info(f"Initial conversion completed using: {conversion_result.get('method_used', 'unknown')}")
            
        except Exception as e:
            logger.error(f"Initial conversion failed: {str(e)}")
            return {
                'success': False,
                'final_query': postgresql_query,
                'error': f"Conversion failed: {str(e)}",
                'attempts': [],
                'suggestions': ["Check the PostgreSQL query syntax"]
            }
        
        # Now test and fix the converted query
        for attempt in range(1, max_correction_attempts + 1):
            logger.info(f"Attempt {attempt}: Testing query")
            
            success, result = await self.firebolt_client.execute_query(converted_query)
            
            if success:
                logger.info(f"✅ Query succeeded on attempt {attempt}!")
                attempts.append({
                    'attempt': attempt,
                    'query': converted_query,
                    'converted_query': converted_query,  # Frontend expects this field name
                    'success': True,  # Frontend expects 'success' not 'status'
                    'status': 'success',
                    'result': result
                })
                
                return {
                    'success': True,
                    'final_status': 'success',  # Frontend expects this
                    'final_query': converted_query,
                    'final_result': result,  # Add final_result for frontend
                    'result': result,
                    'attempts': attempts,
                    'total_attempts': attempt
                }
            
            # Query failed, record the attempt
            error_message = result.get('error', 'Unknown error')
            logger.warning(f"Attempt {attempt} failed: {error_message}")
            
            # Track this error
            if error_message in error_history:
                error_history[error_message].append(attempt)
                logger.warning(f"🔄 REPEATED ERROR seen in attempts: {error_history[error_message]}")
            else:
                error_history[error_message] = [attempt]
                logger.info(f"🆕 NEW ERROR first seen in attempt {attempt}")
            
            attempts.append({
                'attempt': attempt,
                'query': converted_query,
                'converted_query': converted_query,  # Frontend expects this field name
                'success': False,  # Frontend expects 'success' not 'status'
                'status': 'failed',
                'error': error_message
            })
            
            # Don't try to fix on the last attempt
            if attempt < max_correction_attempts:
                # Try to get a correction - pass error history for context
                corrected_query = await self._get_correction(converted_query, error_message, attempt, error_history)
                
                if corrected_query and corrected_query != converted_query:
                    logger.info(f"✅ Got correction for attempt {attempt}")
                    converted_query = corrected_query
                else:
                    logger.warning(f"❌ No correction available for attempt {attempt}")
                    # Even if no correction, continue with the same query (sometimes retrying helps)
        
        # All attempts failed
        logger.error(f"❌ All {max_correction_attempts} attempts failed")
        
        return {
            'success': False,
            'final_status': 'failed',  # Frontend expects this
            'final_query': converted_query,
            'error': f"Failed after {max_correction_attempts} attempts",
            'attempts': attempts,
            'total_attempts': max_correction_attempts,  # Add total_attempts
            'suggestions': self._generate_manual_fix_suggestions(attempts)
        }
    
    async def _get_correction(self, query: str, error_message: str, attempt: int, error_history: Dict[str, List[int]]) -> Optional[str]:
        """Get correction from OpenAI - Intelligent about repeated errors"""
        
        if not self.openai_api_key:
            logger.warning("❌ No OpenAI API key available for correction")
            return None
            
        try:
            import openai
            
            # Set API key
            openai.api_key = self.openai_api_key
            
            # Check if this is a repeated error
            is_repeated_error = len(error_history.get(error_message, [])) > 1
            previous_attempts = error_history.get(error_message, [])
            
            # Analyze the specific error to provide targeted guidance
            specific_guidance = self._get_specific_error_guidance(error_message)
            
            if is_repeated_error:
                logger.warning(f"🔄 REPEATED ERROR: This same error occurred in attempts: {previous_attempts}")
                # For repeated errors, be more explicit and demanding
                prompt = f"""🚨 CRITICAL: This Firebolt query has FAILED {len(previous_attempts)} times with the SAME ERROR.

⚠️ ERROR MESSAGE (REPEATED): {error_message}
Previous failed attempts: {previous_attempts}
Current attempt: {attempt}

🔥 FAILING QUERY:
{query}

⚠️ IMPORTANT: Your previous {len(previous_attempts)} attempts to fix this error COMPLETELY FAILED. 
The query above is still causing the exact same error. You MUST try a RADICALLY different approach.

{specific_guidance}

🎯 YOU MUST CHANGE THE QUERY SIGNIFICANTLY. Don't just return the same query.
Provide ONLY the corrected Firebolt SQL that will NOT produce this error:"""
            
            else:
                logger.info(f"🆕 NEW ERROR: First time seeing this error")
                # For new errors, be more specific about the exact issue
                prompt = f"""🔧 This Firebolt query failed with a specific error. Fix the exact issue.

❌ ERROR MESSAGE: {error_message}

🔥 FAILING QUERY:
{query}

{specific_guidance}

✅ Provide ONLY the corrected Firebolt SQL that fixes this specific error:"""
            
            logger.info(f"🔧 Sending {'REPEATED' if is_repeated_error else 'NEW'} error to OpenAI for correction (attempt {attempt})")
            logger.info(f"📤 ERROR + QUERY SENT TO OPENAI:")
            logger.info(f"=" * 80)
            logger.info(prompt)
            logger.info(f"=" * 80)
            
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a Firebolt SQL expert. When given an error, you MUST modify the query to fix it. Never return the same query unchanged. Analyze the specific error message carefully and make targeted fixes."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,  # Increase temperature for more varied responses
                max_tokens=3000
            )
            
            corrected = response.choices[0].message.content.strip()
            
            logger.info(f"📥 OPENAI RESPONSE:")
            logger.info(f"=" * 80)
            logger.info(corrected)
            logger.info(f"=" * 80)
            
            # Clean up the response
            corrected = self._clean_sql_response(corrected)
            
            # Check if OpenAI returned the same query
            logger.info(f"📊 COMPARISON:")
            logger.info(f"Original query length: {len(query)}")
            logger.info(f"OpenAI response length: {len(corrected)}")
            logger.info(f"Are they identical? {query.strip() == corrected.strip()}")
            
            # Additional cleanup 
            corrected_cleaned = self._normalize_sql(corrected)
            original_cleaned = self._normalize_sql(query)
            
            logger.info(f"📝 AFTER CLEANUP:")
            logger.info(f"Cleaned query length: {len(corrected_cleaned)}")
            logger.info(f"Are they identical after cleanup? {original_cleaned == corrected_cleaned}")
            
            # If OpenAI returned same/empty query, return None to skip this attempt
            if not corrected.strip() or corrected_cleaned == original_cleaned:
                logger.warning(f"⚠️ OpenAI returned same/empty query:")
                logger.warning(f"   - Original query preview: {query[:100]}...")
                logger.warning(f"   - OpenAI response preview: {corrected[:100]}...")
                logger.warning(f"   - Empty response? {not corrected.strip()}")
                logger.warning(f"   - Identical? {corrected_cleaned == original_cleaned}")
                return None
            
            return corrected
            
        except Exception as e:
            logger.error(f"OpenAI correction failed: {str(e)}")
            return None

    def _get_specific_error_guidance(self, error_message: str) -> str:
        """Provide specific guidance based on the exact error message"""
        
        # Convert to lowercase for easier matching
        error_lower = error_message.lower()
        
        if "extract()" in error_lower and "date, timestamp, or timestamptz" in error_lower:
            return """🛠️ SPECIFIC FIX FOR EXTRACT ERROR:
The error says EXTRACT() needs DATE, TIMESTAMP, or TIMESTAMPTZ input.
- Find the EXTRACT() function in your query
- The input column needs to be cast to proper date/timestamp type
- Use: EXTRACT(YEAR FROM CAST(your_column AS TIMESTAMP))
- DO NOT use JSON functions for this - this is a date casting issue
- Example: EXTRACT(YEAR FROM now()) → EXTRACT(YEAR FROM CAST(date_column AS TIMESTAMP))"""
        
        elif "json_pointer_extract_text" in error_lower and "double precision" in error_lower:
            return """🛠️ SPECIFIC FIX FOR JSON FUNCTION ERROR:
You're trying to use JSON_POINTER_EXTRACT_TEXT on a NUMERIC column!
- JSON_POINTER_EXTRACT_TEXT only works on TEXT columns containing JSON
- You have a NUMERIC (double precision) column, not a JSON column
- REMOVE the JSON_POINTER_EXTRACT_TEXT wrapper entirely
- Just use the column directly: column_name (not JSON_POINTER_EXTRACT_TEXT(column_name, '/'))
- Example: JSON_POINTER_EXTRACT_TEXT(amount, '/') → amount"""
        
        elif "jsonextract" in error_lower:
            return """🛠️ SPECIFIC FIX FOR JSONExtract ERROR:
- JSONExtract() does not exist in Firebolt
- Use JSON_POINTER_EXTRACT_TEXT(column, '/path') for JSON data
- But ONLY if the column actually contains JSON text data
- For regular columns, don't use JSON functions at all"""
        
        elif "filter" in error_lower and "not supported" in error_lower:
            return """🛠️ SPECIFIC FIX FOR FILTER ERROR:
- FILTER clause is not supported in Firebolt
- Convert: SUM(amount) FILTER (WHERE condition)
- To: SUM(CASE WHEN condition THEN amount ELSE 0 END)"""
        
        elif "function signature" in error_lower and "not found" in error_lower:
            return """🛠️ SPECIFIC FIX FOR FUNCTION SIGNATURE ERROR:
- You're using a function with wrong parameter types
- Check the function documentation for correct parameter types
- Make sure you're not mixing JSON functions with numeric columns
- Cast parameters to the correct type if needed"""
        
        else:
            return """🛠️ GENERAL TROUBLESHOOTING:
- Read the error message carefully and fix the exact issue mentioned
- Don't apply generic fixes that don't match the specific error
- Test your understanding: what is the error actually saying?
- Make targeted changes, don't change unrelated parts of the query"""

    def _clean_sql_response(self, response: str) -> str:
        """Clean up OpenAI response by removing markdown formatting and extra whitespace"""
        if not response:
            return ""
            
        # Remove markdown code blocks
        if response.startswith('```sql'):
            response = response[6:]
        elif response.startswith('```'):
            response = response[3:]
        if response.endswith('```'):
            response = response[:-3]
            
        return response.strip()

    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL for comparison by removing extra whitespace and standardizing case"""
        if not sql:
            return ""
            
        # Remove extra whitespace and normalize
        normalized = ' '.join(sql.strip().split())
        # Remove semicolons for comparison
        normalized = normalized.rstrip(';')
        return normalized.lower()
    
    async def _get_openai_correction(self, query: str, error_message: str) -> Optional[str]:
        """REMOVED - Only using simplified OpenAI approach now"""
        return None
    
    def _apply_rule_based_fixes(self, query: str, error_message: str) -> str:
        """REMOVED - Only using OpenAI approach now"""
        logger.info("⚠️ Rule-based fixes disabled - using OpenAI only")
        return query
    
    def _generate_manual_fix_suggestions(self, attempts: List[Dict]) -> List[str]:
        """Generate manual fix suggestions based on failed attempts"""
        suggestions = []
        
        # Analyze common error patterns
        for attempt in attempts:
            error = attempt.get('error', '')
            
            if 'extract()' in error.lower():
                suggestions.append("EXTRACT function requires DATE/TIMESTAMP input. Use CAST(column AS DATE) inside EXTRACT.")
            
            if 'syntax error' in error.lower():
                suggestions.append("Check SQL syntax, especially parentheses and subquery structure.")
            
            if '::' in error:
                suggestions.append("PostgreSQL casting (::) not supported. Use CAST(value AS TYPE) syntax.")
        
        if not suggestions:
            suggestions.append("Consider using a different engine or checking Firebolt documentation.")
            
        return list(set(suggestions))  # Remove duplicates
    
    async def test_connection(self) -> Tuple[bool, str]:
        """Test the Firebolt connection"""
        return await self.firebolt_client.test_connection()
    
    def get_test_statistics(self, test_session: Dict) -> Dict:
        """Generate statistics from a test session"""
        stats = {
            "total_attempts": test_session["total_attempts"],
            "success": test_session["success"],
            "final_status": "SUCCESS" if test_session["success"] else "FAILED",
            "error_types_encountered": [],
            "correction_methods_used": [],
            "execution_time_total_ms": 0
        }
        
        for attempt in test_session["attempts"]:
            if not attempt["execution_success"]:
                error_type = attempt.get("error_analysis", {}).get("error_type")
                if error_type and error_type not in stats["error_types_encountered"]:
                    stats["error_types_encountered"].append(error_type)
            
            correction_method = attempt.get("correction_method")
            if correction_method and correction_method not in stats["correction_methods_used"]:
                stats["correction_methods_used"].append(correction_method)
                
            exec_time = attempt.get("execution_result", {}).get("execution_time_ms", 0)
            stats["execution_time_total_ms"] += exec_time
        
        return stats 