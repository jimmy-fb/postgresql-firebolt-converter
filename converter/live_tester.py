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
            
            if is_repeated_error:
                logger.warning(f"🔄 REPEATED ERROR: This same error occurred in attempts: {previous_attempts}")
                # For repeated errors, be more explicit
                prompt = f"""This Firebolt query has the SAME ERROR REPEATING across multiple attempts.

ERROR MESSAGE (REPEATED): {error_message}
Previous failed attempts with this same error: {previous_attempts}
Current attempt: {attempt}

FAILING QUERY:
{query}

⚠️ IMPORTANT: Your previous attempts to fix this error FAILED. Please try a DIFFERENT approach.

Fix using correct Firebolt syntax:
- JSON functions: JSONExtract() does NOT exist, use JSON_POINTER_EXTRACT_TEXT(column, '/path')
- JSON paths: use /path (not $.path)  
- EXTRACT functions: wrap with CAST AS DATE/TIMESTAMP
- FILTER clauses: use CASE WHEN instead
- PostgreSQL casting: use CAST() not ::
- Missing columns: add proper JOINs or remove references
- See: https://docs.firebolt.io/reference-sql/functions-reference/json

Since this error is REPEATING, try a completely different approach. 
Please provide ONLY the corrected Firebolt SQL query:"""
            else:
                logger.info(f"🆕 NEW ERROR: First time seeing this error")
                # For new errors, use standard prompt
                prompt = f"""This Firebolt query failed with a specific error. Please fix it.

ERROR MESSAGE: {error_message}

FAILING QUERY:
{query}

Fix using correct Firebolt syntax:
- JSON functions: JSONExtract() does NOT exist, use JSON_POINTER_EXTRACT_TEXT(column, '/path')
- JSON paths: use /path (not $.path)  
- EXTRACT functions: wrap with CAST AS DATE/TIMESTAMP
- FILTER clauses: use CASE WHEN instead
- PostgreSQL casting: use CAST() not ::
- See: https://docs.firebolt.io/reference-sql/functions-reference/json

Please provide ONLY the corrected Firebolt SQL query that fixes this specific error:"""
            
            logger.info(f"🔧 Sending {'REPEATED' if is_repeated_error else 'NEW'} error to OpenAI for correction (attempt {attempt})")
            logger.info(f"📤 ERROR + QUERY SENT TO OPENAI:")
            logger.info(f"=" * 80)
            logger.info(prompt)
            logger.info(f"=" * 80)
            
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a Firebolt SQL expert. Fix queries using correct Firebolt syntax. JSONExtract() does not exist - use JSON_POINTER_EXTRACT_TEXT() instead. If an error is repeating, try a completely different approach."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=2000
            )
            
            corrected = response.choices[0].message.content.strip()
            
            logger.info(f"📥 OPENAI RESPONSE:")
            logger.info(f"=" * 80)
            logger.info(corrected)
            logger.info(f"=" * 80)
            
            # Show detailed comparison
            logger.info(f"📊 COMPARISON:")
            logger.info(f"Original query length: {len(query)}")
            logger.info(f"OpenAI response length: {len(corrected)}")
            logger.info(f"Are they identical? {corrected == query}")
            
            # Clean up any markdown formatting
            if corrected.startswith('```sql'):
                corrected = corrected[6:]
            elif corrected.startswith('```'):
                corrected = corrected[3:]
            if corrected.endswith('```'):
                corrected = corrected[:-3]
                
            corrected = corrected.strip()
            
            logger.info(f"📝 AFTER CLEANUP:")
            logger.info(f"Cleaned query length: {len(corrected)}")
            logger.info(f"Are they identical after cleanup? {corrected == query}")
            
            if corrected and corrected != query:
                logger.info("✅ Got OpenAI conversion to Firebolt")
                return corrected
            else:
                logger.warning("⚠️ OpenAI returned same/empty query:")
                logger.warning(f"   - Original query preview: {query[:100]}...")
                logger.warning(f"   - OpenAI response preview: {corrected[:100]}...")
                logger.warning(f"   - Empty response? {not corrected}")
                logger.warning(f"   - Identical? {corrected == query}")
                return None
                
        except Exception as e:
            logger.error(f"❌ OpenAI conversion failed: {str(e)}")
            return None
    
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