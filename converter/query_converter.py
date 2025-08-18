import re
import json
from typing import Dict, List, Tuple, Optional
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
                import openai  # type: ignore
                openai.api_key = self.openai_api_key
                self.openai_available = True
            except Exception:
                print("OpenAI package not available. AI features will be disabled.")
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
        """Convert PostgreSQL query to Firebolt SQL using rule-based conversions first.

        Optionally polish with AI if explicitly enabled and available.
        """
        cleaned_query = postgresql_query.strip()
        warnings: List[str] = []
        explanations: List[str] = []

        # 1) Apply deterministic, rule-based conversions
        converted_sql = self._apply_rule_based_conversion(cleaned_query, explanations, warnings)

        # 2) Optionally apply AI polish if requested via env flag
        use_ai_polish = os.getenv('ENABLE_AI_POLISH', 'false').lower() in ('1', 'true', 'yes')
        if use_ai_polish and self.openai_available:
            try:
                ai_sql = self._apply_ai_conversion(converted_sql)
                if ai_sql and isinstance(ai_sql, str):
                    converted_sql = ai_sql
                    explanations.append("Applied optional AI polish using OpenAI")
                    method_used = 'rule_based+ai'
                else:
                    method_used = 'rule_based'
            except Exception as e:
                warnings.append(f"AI polish skipped due to error: {str(e)}")
                method_used = 'rule_based'
        else:
            method_used = 'rule_based'

        return {
            'converted_sql': converted_sql,
            'warnings': warnings,
            'explanations': explanations,
            'method_used': method_used
        }

    def _apply_ai_conversion(self, sql: str) -> str:
        """Apply AI-powered conversion using OpenAI - Simple and Direct"""
        try:
            import openai  # type: ignore
            # Enhanced prompt with Firebolt documentation references
            prompt = f"""Convert this PostgreSQL query to be fully Firebolt compliant using the official Firebolt documentation.

🔗 REFERENCE THE OFFICIAL FIREBOLT DOCUMENTATION:
- Main SQL Reference: https://docs.firebolt.io/reference-sql/
- Functions: https://docs.firebolt.io/reference-sql/functions-reference/
- JSON Functions: https://docs.firebolt.io/reference-sql/functions-reference/json
- Date/Time Functions: https://docs.firebolt.io/reference-sql/functions-reference/date-time
- Data Types: https://docs.firebolt.io/reference-sql/data-types/
- Aggregation Functions: https://docs.firebolt.io/reference-sql/functions-reference/aggregation

Fix ALL Firebolt compatibility issues including:

📅 EXTRACT/DATE FUNCTIONS - CRITICAL SUBQUERY FIX:
- EXTRACT functions: wrap expressions with CAST AS DATE/TIMESTAMP/TIMESTAMPTZ
- Correct syntax: EXTRACT(part FROM CAST(column AS TIMESTAMP))

🚨 EXTRACT WITH SUBQUERIES - SPECIAL PATTERN:
❌ WRONG: EXTRACT(MONTH FROM (SELECT MAX(date_col) FROM table))
✅ CORRECT: Pull subquery out as cross join:
```sql
FROM main_table,
(SELECT MAX(date_col) AS max_date FROM table) AS sub
WHERE EXTRACT(MONTH FROM CAST(main_col AS DATE)) = EXTRACT(MONTH FROM sub.max_date)
```

WORKING EXAMPLE:
- Bad: WHERE EXTRACT(MONTH FROM (SELECT MAX(agreementdate::date) from jayam_contract_details))
- Good: FROM table, (SELECT MAX(agreementdate::DATE) AS max_date FROM jayam_contract_details) AS sub WHERE EXTRACT(MONTH FROM sub.max_date)

- Reference: https://docs.firebolt.io/reference-sql/functions-reference/date-time#extract

🔢 AGGREGATION:
- FILTER clauses (PostgreSQL) → CASE WHEN (Firebolt)
- Convert: SUM(x) FILTER (WHERE y) → SUM(CASE WHEN y THEN x ELSE 0 END)
- Reference: https://docs.firebolt.io/reference-sql/functions-reference/aggregation

🏷️ CASTING:
- PostgreSQL casting (::) → Firebolt CAST() function
- Convert: column::type → CAST(column AS type)

📋 JSON OPERATIONS - CRITICAL (Use EXACT syntax):
❌ WRONG: JSON_EXTRACT_TEXT(), JSON_EXTRACT_STRING(), JSONExtract()
✅ CORRECT: JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(column, '/path'))

EXACT CONVERSION PATTERN:
- PostgreSQL: object_data::json->>'key'
- Firebolt: JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(object_data, '/key'))

WORKING EXAMPLE:
- PostgreSQL: object_data::json->>'IMD'
- Firebolt: JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(object_data, '/IMD'))

NEVER use: JSON_EXTRACT_TEXT, JSON_EXTRACT_STRING, JSONExtract
ALWAYS use: JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(column, '/path'))
- Reference: https://docs.firebolt.io/reference-sql/functions-reference/json

⏰ TIMESTAMP FUNCTIONS:
- now() → CURRENT_TIMESTAMP or NOW()
- Proper timezone handling with AT TIME ZONE

🔍 SUBQUERIES:
- Check for Firebolt subquery limitations and refactor if needed

PostgreSQL Query:
{sql}

Return ONLY the corrected Firebolt SQL query that follows official Firebolt syntax:"""
            
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "system", 
                        "content": """You are a SQL expert specializing in converting PostgreSQL queries to Firebolt SQL.

IMPORTANT: Always reference the official Firebolt documentation at https://docs.firebolt.io/ for accurate syntax and function signatures.

🚨 CRITICAL JSON CONVERSION RULE:
PostgreSQL: object_data::json->>'key'
Firebolt: JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(object_data, '/key'))

❌ NEVER use these (they don't exist): JSON_EXTRACT_TEXT, JSON_EXTRACT_STRING, JSONExtract
✅ ALWAYS use: JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(column, '/path'))

Key References:
- SQL Functions: https://docs.firebolt.io/reference-sql/functions-reference/
- JSON Functions: https://docs.firebolt.io/reference-sql/functions-reference/json
- Date/Time Functions: https://docs.firebolt.io/reference-sql/functions-reference/date-time
- Data Types: https://docs.firebolt.io/reference-sql/data-types/

Firebolt uses a PostgreSQL-compliant SQL dialect but has specific function signatures and requirements. Always provide syntactically correct Firebolt SQL based on the official documentation."""
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=2000
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

    def _apply_rule_based_conversion(self, sql: str, explanations: List[str], warnings: List[str]) -> str:
        """Apply rule-based conversions including JSON ops, datatypes, functions, and subquery refactors."""
        original_sql = sql

        # Function-level rewrites (FILTER -> CASE WHEN, NOW timezone, remove ::json, etc.)
        sql = self.rules.apply_patterns(sql, 'functions')
        if sql != original_sql:
            explanations.append("Normalized function usage (FILTER -> CASE WHEN, NOW timezone, POSITION -> STRPOS, etc.)")
            original_sql = sql

        # JSON operators and paths after removing ::json/::jsonb
        sql = self.rules.apply_patterns(sql, 'json')
        if sql != original_sql:
            explanations.append("Converted PostgreSQL JSON operators to Firebolt JSON_VALUE/JSON_POINTER_EXTRACT_TEXT")
            original_sql = sql

        # Data type normalizations (:: casting, uppercase types)
        sql = self.rules.apply_patterns(sql, 'datatypes')
        if sql != original_sql:
            explanations.append("Normalized data types and :: casting style for Firebolt")
            original_sql = sql

        # Critical: Refactor scalar subqueries inside EXTRACT()
        refactored_sql, refactor_info = self._refactor_extract_scalar_subqueries(sql)
        if refactored_sql != sql:
            sql = refactored_sql
            explanations.append("Refactored scalar subquery inside EXTRACT() into derived subquery in FROM")
            if refactor_info.get('notes'):
                warnings.extend(refactor_info['notes'])

        # Collect warnings for other unsupported constructs detected
        warnings.extend(self.rules.detect_unsupported_features(sql))

        return sql

    def _refactor_extract_scalar_subqueries(self, sql: str) -> Tuple[str, Dict[str, List[str]]]:
        """Refactor patterns like EXTRACT(MONTH FROM (SELECT MAX(col::date) FROM table))
        into a derived subquery added to FROM and replace inline EXTRACT references.

        This is a heuristic implementation aimed at the known problematic patterns.
        """
        notes: List[str] = []
        subqueries_to_add: List[Tuple[str, str]] = []  # list of (subquery_sql, alias)
        replacements: List[Tuple[str, str]] = []       # list of (old, new)

        # Regex to capture EXTRACT(part FROM (SELECT MAX(expr) FROM table ...))
        pattern = re.compile(
            r"EXTRACT\s*\(\s*(MONTH|YEAR)\s+FROM\s*\(\s*SELECT\s+MAX\s*\(\s*([^\)]+?)\s*\)\s+from\s+([^)]+?)\s*\)\s*\)",
            re.IGNORECASE | re.DOTALL
        )

        alias_counter = 1
        used_aliases: List[str] = []

        def build_alias(expr: str) -> str:
            expr_l = expr.lower()
            if 'agreementdate' in expr_l:
                return 'max_agreement_date'
            # Fallback generic alias
            return 'max_value'

        def ensure_unique_alias(alias_base: str) -> str:
            nonlocal alias_counter
            alias = alias_base
            while alias in used_aliases:
                alias = f"{alias_base}_{alias_counter}"
                alias_counter += 1
            used_aliases.append(alias)
            return alias

        matches = list(pattern.finditer(sql))
        if not matches:
            return sql, {'notes': notes}

        # Map each distinct scalar subquery to an alias so we can reuse
        seen_inner_to_alias: Dict[str, Tuple[str, str]] = {}

        for m in matches:
            part = m.group(1)
            expr = m.group(2).strip()
            table_part = m.group(3).strip()

            inner_select = f"SELECT MAX({expr}) FROM {table_part}"
            if inner_select not in seen_inner_to_alias:
                col_alias = ensure_unique_alias(build_alias(expr))
                sub_alias = 'sub' if 'sub' not in used_aliases else ensure_unique_alias('sub')
                derived_sql = f"(SELECT MAX({expr}) AS {col_alias} FROM {table_part}) AS {sub_alias}"
                seen_inner_to_alias[inner_select] = (sub_alias, col_alias)
                subqueries_to_add.append((derived_sql, sub_alias))

            sub_alias, col_alias = seen_inner_to_alias[inner_select]

            # Replacement: EXTRACT(part FROM (SELECT MAX(expr) FROM table)) -> EXTRACT(part FROM sub_alias.col_alias)
            full_old = m.group(0)
            full_new = f"EXTRACT({part} FROM {sub_alias}.{col_alias})"
            replacements.append((full_old, full_new))

        # Apply replacements to change EXTRACT(...) calls
        refactored = sql
        for old, new in replacements:
            refactored = refactored.replace(old, new)

        # Add derived subqueries to FROM clause
        if subqueries_to_add:
            subquery_clause = ", ".join([sq for sq, _ in subqueries_to_add])
            
            # Find the main FROM clause (not EXTRACT FROM)
            # Look for FROM that follows table patterns and isn't part of EXTRACT
            from_matches = []
            for match in re.finditer(r'\bFROM\b', refactored, re.IGNORECASE):
                # Check if this FROM is part of an EXTRACT function
                before_from = refactored[:match.start()].strip()
                if re.search(r'EXTRACT\s*\(\s*(?:MONTH|YEAR|DAY|HOUR|MINUTE|SECOND)\s*$', before_from, re.IGNORECASE):
                    continue  # Skip EXTRACT FROM
                from_matches.append(match)
            
            if from_matches:
                # Use the first valid FROM clause (main table FROM)
                main_from = from_matches[0]
                insertion = ", " + subquery_clause
                
                # Find where current FROM clause ends (before WHERE/GROUP/ORDER/LIMIT/HAVING)
                search_start = main_from.end()
                boundary = len(refactored)
                
                for kw in [r"\bWHERE\b", r"\bGROUP\s+BY\b", r"\bORDER\s+BY\b", r"\bLIMIT\b", r"\bHAVING\b"]:
                    m_kw = re.search(kw, refactored[search_start:], re.IGNORECASE)
                    if m_kw:
                        boundary = min(boundary, search_start + m_kw.start())
                
                # Insert with proper spacing
                before_insert = refactored[:boundary].rstrip()
                after_insert = refactored[boundary:].lstrip()
                space_after = " " if after_insert and not after_insert.startswith('\n') else ""
                
                refactored = before_insert + insertion + space_after + after_insert
            else:
                # No FROM clause - need to add one after the SELECT part
                select_match = re.search(r"\bSELECT\b", refactored, re.IGNORECASE)
                if select_match:
                    # Find where SELECT ends (before WHERE/GROUP/ORDER/LIMIT/semicolon)
                    select_start = select_match.end()
                    boundary = len(refactored)
                    
                    for kw in [r"\bWHERE\b", r"\bGROUP\s+BY\b", r"\bORDER\s+BY\b", r"\bLIMIT\b", r"\bHAVING\b", r";"]:
                        m_kw = re.search(kw, refactored[select_start:], re.IGNORECASE)
                        if m_kw:
                            boundary = min(boundary, select_start + m_kw.start())
                    
                    # Split and add FROM clause
                    select_part = refactored[:boundary].rstrip()
                    rest_part = refactored[boundary:].lstrip()
                    space_after = " " if rest_part and not rest_part.startswith('\n') and not rest_part.startswith(';') else ""
                    
                    refactored = select_part + f" FROM {subquery_clause}" + space_after + rest_part
                else:
                    notes.append("Could not find SELECT clause to add FROM with derived subquery.")

        return refactored, {'notes': notes}
    
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

            # Import on-demand to avoid hard dependency
            import openai  # type: ignore
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