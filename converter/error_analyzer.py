"""
Firebolt Error Analysis and Auto-Correction

This module analyzes Firebolt query execution errors and uses the MCP server
to understand the issues and suggest automatic corrections.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from .mcp_client import FireboltMCPClient

logger = logging.getLogger(__name__)

class FireboltErrorAnalyzer:
    """Analyzes Firebolt errors and suggests corrections using MCP server"""
    
    def __init__(self, mcp_client: FireboltMCPClient = None):
        self.mcp_client = mcp_client
        
        # Common Firebolt error patterns and their fixes
        self.error_patterns = {
            "function.*not found": {
                "type": "unknown_function",
                "description": "Function not supported in Firebolt",
                "auto_fix": True
            },
            "syntax error.*unexpected": {
                "type": "syntax_error", 
                "description": "SQL syntax not supported in Firebolt",
                "auto_fix": True
            },
            "column.*does not exist": {
                "type": "column_error",
                "description": "Column reference issue",
                "auto_fix": False
            },
            "table.*does not exist": {
                "type": "table_error",
                "description": "Table reference issue", 
                "auto_fix": False
            },
            "scalar subquery": {
                "type": "subquery_error",
                "description": "Scalar subquery not supported in this context",
                "auto_fix": True
            },
            "filter.*not supported": {
                "type": "filter_error",
                "description": "FILTER clause not supported",
                "auto_fix": True
            }
        }
    
    def analyze_error(self, error_message: str, query: str, error_code: str = None) -> Dict:
        """
        Analyze a Firebolt error and determine the issue type and potential fixes
        
        Args:
            error_message: The error message from Firebolt
            query: The SQL query that caused the error
            error_code: Optional error code from Firebolt
            
        Returns:
            Dict with analysis results and suggested fixes
        """
        analysis = {
            "error_message": error_message,
            "error_code": error_code,
            "error_type": "unknown",
            "description": "Unknown error",
            "can_auto_fix": False,
            "suggested_fixes": [],
            "confidence": 0
        }
        
        # Pattern matching against known errors
        error_lower = error_message.lower()
        
        for pattern, info in self.error_patterns.items():
            if re.search(pattern, error_lower):
                analysis.update({
                    "error_type": info["type"],
                    "description": info["description"],
                    "can_auto_fix": info["auto_fix"],
                    "confidence": 85
                })
                break
        
        # Generate specific fixes based on error type
        if analysis["error_type"] == "unknown_function":
            analysis["suggested_fixes"] = self._suggest_function_fixes(error_message, query)
        elif analysis["error_type"] == "syntax_error":
            analysis["suggested_fixes"] = self._suggest_syntax_fixes(error_message, query)
        elif analysis["error_type"] == "subquery_error":
            analysis["suggested_fixes"] = self._suggest_subquery_fixes(error_message, query)
        elif analysis["error_type"] == "filter_error":
            analysis["suggested_fixes"] = self._suggest_filter_fixes(error_message, query)
        
        return analysis
    
    def _suggest_function_fixes(self, error: str, query: str) -> List[str]:
        """Suggest fixes for unknown function errors"""
        fixes = []
        
        # Extract function name from error
        func_match = re.search(r"function\s+['\"]?(\w+)['\"]?\s+", error.lower())
        if func_match:
            func_name = func_match.group(1)
            
            # Common PostgreSQL to Firebolt function mappings
            function_mappings = {
                "position": "STRPOS",
                "date_trunc": "DATE_TRUNC",
                "extract": "EXTRACT",
                "substring": "SUBSTR",
                "length": "LEN",
                "current_timestamp": "NOW()",
                "generate_series": "Use VALUES clause or recursive CTE alternative"
            }
            
            if func_name in function_mappings:
                replacement = function_mappings[func_name]
                fixes.append(f"Replace {func_name.upper()} with {replacement}")
                
                # Generate corrected query
                corrected_query = re.sub(
                    f"\\b{func_name}\\b", 
                    replacement.split()[0] if " " not in replacement else func_name,
                    query, 
                    flags=re.IGNORECASE
                )
                fixes.append(f"Suggested query: {corrected_query}")
        
        return fixes
    
    def _suggest_syntax_fixes(self, error: str, query: str) -> List[str]:
        """Suggest fixes for syntax errors"""
        fixes = []
        
        if "filter" in error.lower():
            fixes.append("Convert FILTER clause to CASE WHEN expression")
            fixes.append("Example: SUM(amount) FILTER (WHERE active) → SUM(CASE WHEN active THEN amount ELSE 0 END)")
            
        if "lateral" in error.lower():
            fixes.append("LATERAL joins not supported - use regular JOIN with subquery")
            
        if "::" in query and "casting" in error.lower():
            fixes.append("Convert :: casting to CAST() function")
            fixes.append("Example: column::text → CAST(column AS TEXT)")
        
        return fixes
    
    def _suggest_subquery_fixes(self, error: str, query: str) -> List[str]:
        """Suggest fixes for subquery errors"""
        fixes = []
        
        if "extract" in query.lower() and "select" in query.lower():
            fixes.append("Move scalar subquery from EXTRACT to derived table in FROM clause")
            fixes.append("Example: EXTRACT(YEAR FROM (SELECT MAX(date)...)) → use derived subquery")
            
        fixes.append("Refactor scalar subqueries to use JOINs or derived tables")
        return fixes
    
    def _suggest_filter_fixes(self, error: str, query: str) -> List[str]:
        """Suggest fixes for FILTER clause errors"""
        fixes = []
        
        # Find FILTER clauses and suggest CASE WHEN alternatives
        filter_pattern = r'(\w+)\s*\([^)]+\)\s*FILTER\s*\(\s*WHERE\s+([^)]+)\)'
        matches = re.findall(filter_pattern, query, re.IGNORECASE)
        
        for agg_func, condition in matches:
            case_when = f"{agg_func}(CASE WHEN {condition} THEN column_name ELSE 0 END)"
            fixes.append(f"Convert {agg_func} FILTER → {case_when}")
        
        return fixes
    
    async def get_mcp_correction(self, query: str, error_message: str) -> str:
        """
        Get correction suggestion from MCP server
        
        Args:
            query: The failed SQL query
            error_message: Error message from Firebolt
            
        Returns:
            str: Corrected query or error message
        """
        try:
            if self.mcp_client and hasattr(self.mcp_client, 'get_expert_correction'):
                correction = await self.mcp_client.get_expert_correction(query, error_message)
                return correction
            else:
                return "MCP client not available or missing get_expert_correction method"
                
        except Exception as e:
            logger.error(f"MCP correction failed: {str(e)}")
            return f"MCP correction failed: {str(e)}"
    
    def generate_correction_summary(self, original_query: str, corrected_query: str, 
                                  error_analysis: Dict) -> Dict:
        """Generate a summary of what was corrected"""
        return {
            "original_query": original_query,
            "corrected_query": corrected_query,
            "error_type": error_analysis.get("error_type"),
            "description": error_analysis.get("description"),
            "changes_made": self._identify_changes(original_query, corrected_query),
            "confidence": error_analysis.get("confidence", 0)
        }
    
    def _identify_changes(self, original: str, corrected: str) -> List[str]:
        """Identify what changes were made between queries"""
        changes = []
        
        # Simple change detection - could be enhanced with proper diff
        if "filter" in original.lower() and "case when" in corrected.lower():
            changes.append("Converted FILTER clause to CASE WHEN")
            
        if "::" in original and "cast(" in corrected.lower():
            changes.append("Converted :: casting to CAST() function")
            
        if "extract(" in original.lower() and "from (" in corrected.lower():
            changes.append("Refactored scalar subquery to derived table")
        
        return changes if changes else ["Unknown changes made"] 