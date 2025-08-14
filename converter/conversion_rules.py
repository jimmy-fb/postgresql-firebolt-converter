import re
from typing import List, Tuple

class ConversionRules:
    def __init__(self):
        self.json_patterns = self._compile_json_patterns()
        self.datatype_patterns = self._compile_datatype_patterns()
        self.function_patterns = self._compile_function_patterns()
        
    def _compile_json_patterns(self) -> List[Tuple[re.Pattern, str]]:
        """Compile regex patterns for JSON operations with correct Firebolt syntax"""
        return [
            # ONLY match actual JSON operators -> and ->>
            # Pattern: column->>'key' or column->"key" (with quotes around key)
            (re.compile(r"(\w+(?:\.\w+)?)\s*->>\s*['\"]([^'\"]+)['\"]"), 
             lambda m: f"JSON_VALUE(JSON_POINTER_EXTRACT_TEXT({m.group(1)}, '/{m.group(2)}'))"),
            
            # Pattern: column->'key' or column->"key" (with quotes around key) 
            (re.compile(r"(\w+(?:\.\w+)?)\s*->\s*['\"]([^'\"]+)['\"]"), 
             lambda m: f"JSON_VALUE(JSON_POINTER_EXTRACT_TEXT({m.group(1)}, '/{m.group(2)}'))"),
            
            # Pattern: column#>'{key1,key2}' -> JSON path array
            (re.compile(r"(\w+(?:\.\w+)?)\s*#>>\s*'\{([^}]+)\}'"),
             lambda m: f"JSON_VALUE(JSON_POINTER_EXTRACT_TEXT({m.group(1)}, '/{m.group(2).replace(',', '/')}'))"),
              
            # Pattern: column#>'{key1,key2}' -> JSON path array
            (re.compile(r"(\w+(?:\.\w+)?)\s*#>\s*'\{([^}]+)\}'"),
             lambda m: f"JSON_VALUE(JSON_POINTER_EXTRACT_TEXT({m.group(1)}, '/{m.group(2).replace(',', '/')}'))"),
            
            # Fix any existing invalid syntax patterns (only if they already exist)
            (re.compile(r"(\w+)::JSON_EXTRACT\(json,\s*'\$\.([^']+)'\)"),
             lambda m: f"JSON_VALUE(JSON_POINTER_EXTRACT_TEXT({m.group(1)}, '/{m.group(2)}'))"),
        ]
    
    def _compile_datatype_patterns(self) -> List[Tuple[re.Pattern, str]]:
        """Compile regex patterns for data type conversions"""
        return [
            # Convert CAST(expr AS type) to expr::type (Firebolt prefers :: syntax)
            (re.compile(r"CAST\(\s*(.+?)\s+AS\s+(\w+)\s*\)", re.IGNORECASE),
             lambda m: f"{m.group(1)}::{m.group(2).upper()}"),
            
            # Clean up CAST around JSON_VALUE patterns - remove extra parentheses
            (re.compile(r"CAST\(\s*(JSON_VALUE\([^)]+\))\s*\)", re.IGNORECASE),
             lambda m: f"{m.group(1)}"),
             
            # Avoid removing parentheses around casts inside function calls (kept disabled)
             
            # Clean up mixed syntax: CAST(expr)::TYPE -> expr::TYPE
            (re.compile(r"CAST\(\s*([^)]+)\s*\)::([\w_]+)", re.IGNORECASE),
             lambda m: f"({m.group(1)})::{m.group(2).upper()}"),
             
            # Clean up double casting: (expr::TYPE)::TYPE -> expr::TYPE
            (re.compile(r"\(([^)]+)::([\w_]+)\)::([\w_]+)", re.IGNORECASE),
             lambda m: f"({m.group(1)})::{m.group(3).upper()}"),
             
            # PostgreSQL :: casting - keep as is but ensure uppercase type
            (re.compile(r"(\([^)]+\)|[\w.]+)::(varchar|text|integer|int|bigint|decimal|numeric|real|double|float|boolean|date|timestamp|timestamptz)", re.IGNORECASE),
             lambda m: f"{m.group(1)}::{self._map_datatype(m.group(2))}"),
        ]
    
    def _compile_function_patterns(self) -> List[Tuple[re.Pattern, str]]:
        """Compile regex patterns for function conversions"""
        return [
            # CURRENT_TIMESTAMP -> NOW() 
            (re.compile(r'\bCURRENT_TIMESTAMP\b', re.IGNORECASE),
             'NOW()'),
             
            # Add timezone support for NOW() if not present
            (re.compile(r'\bNOW\(\)(?!\s+AT\s+TIME\s+ZONE)', re.IGNORECASE),
             "NOW() AT TIME ZONE 'Asia/Kolkata'"),
             
            # PostgreSQL FILTER clause -> CASE WHEN (critical conversion!)
            (re.compile(r'(\w+)\s*\(\s*([^)]+)\s*\)\s*FILTER\s*\(\s*WHERE\s+([^)]+)\s*\)', re.IGNORECASE),
             lambda m: f"{m.group(1)}(CASE WHEN {m.group(3)} THEN {m.group(2)} ELSE 0 END)"),
             
            # JSON casting ::json (PostgreSQL/Redshift) -> remove before JSON operator conversion
            (re.compile(r'(\w+(?:\.\w+)?)\s*::\s*json\b', re.IGNORECASE),
             lambda m: f"{m.group(1)}"),
             
            # MAX_BUSINESS_DATE() function (specific to user's system)
            (re.compile(r'\bMAX_BUSINESS_DATE\s*\(\s*\)', re.IGNORECASE),
             lambda m: f"/* REPLACE: MAX_BUSINESS_DATE() with actual date/timestamp column */\nMAX(business_date)"),
             
            # Scalar subqueries in EXTRACT - provide guidance
            (re.compile(r'EXTRACT\s*\(\s*(\w+)\s+FROM\s*\(\s*(SELECT\s+[^)]+)\s*\)\s*\)', re.IGNORECASE),
             lambda m: f"/* REFACTOR NEEDED: Move to derived subquery */\nEXTRACT({m.group(1)} FROM ({m.group(2)}))\n/* Should be: EXTRACT({m.group(1)} FROM alias.column) FROM ({m.group(2)}) AS alias */"),
             
            # Position function: position('substr' in 'string') -> STRPOS('string', 'substr')
            (re.compile(r"position\(\s*'([^']+)'\s+in\s+(.+?)\s*\)", re.IGNORECASE),
             lambda m: f"STRPOS({m.group(2)}, '{m.group(1)}')")
        ]
    
    def _map_datatype(self, datatype: str) -> str:
        """Map PostgreSQL data type to Firebolt equivalent"""
        mapping = {
            'varchar': 'TEXT',
            'text': 'TEXT', 
            'integer': 'INT',
            'int': 'INT',
            'bigint': 'BIGINT',
            'decimal': 'DECIMAL',
            'numeric': 'DECIMAL', 
            'real': 'REAL',
            'double': 'DOUBLE',
            'float': 'DOUBLE',
            'boolean': 'BOOLEAN',
            'date': 'DATE',
            'timestamp': 'TIMESTAMP',
            'timestamptz': 'TIMESTAMPTZ'
        }
        return mapping.get(datatype.lower(), datatype.upper())
    
    def apply_patterns(self, sql: str, pattern_type: str) -> str:
        """Apply conversion patterns to SQL"""
        if pattern_type == 'json':
            patterns = self.json_patterns
        elif pattern_type == 'datatypes':
            patterns = self.datatype_patterns
        elif pattern_type == 'functions':
            patterns = self.function_patterns
        else:
            return sql
            
        result = sql
        for pattern, replacement in patterns:
            if callable(replacement):
                result = pattern.sub(replacement, result)
            else:
                result = pattern.sub(replacement, result)
        
        return result
    
    def detect_unsupported_features(self, sql: str) -> List[str]:
        """Detect PostgreSQL-specific features that need warnings"""
        warnings = []
        
        unsupported_patterns = [
            (r'\bCTE\b', "Common Table Expressions (WITH clauses)"),
            (r'\bLATERAL\b', "LATERAL joins"), 
            (r'\bTABLESAMPLE\b', "Table sampling"),
            (r'\bINHERITS\b', "Table inheritance"),
            (r'\bEXCEPT\b', "EXCEPT operator (use LEFT JOIN instead)"),
            (r'\bINTERSECT\b', "INTERSECT operator"),
            (r'\$\$[^$]*\$\$', "Dollar-quoted strings"),
            (r'\bCOPY\b', "COPY command (use Firebolt's COPY FROM instead)"),
            (r'\bVACUUM\b', "VACUUM command (not needed in Firebolt)"),
            (r'\bANALYZE\b', "ANALYZE command (not needed in Firebolt)"),
            (r'\bEXPLAIN\s+\(', "Extended EXPLAIN options"),
            
            # New patterns based on user learnings
            (r'EXTRACT\s*\(\s*\w+\s+FROM\s*\(\s*SELECT\s+', "Scalar subquery inside EXTRACT() - refactor to derived subquery"),
            (r'::\s*json\b', "JSON casting (::json) - use TEXT storage in Firebolt instead"),
            (r'\bFILTER\s*\(\s*WHERE\s+', "FILTER clause in aggregates - converted to CASE WHEN"),
            (r'->\s*\'[^\']+\'', "JSON operator (->) - converted to JSON_VALUE(JSON_POINTER_EXTRACT_TEXT())"),
            (r'->>\s*\'[^\']+\'', "JSON operator (->>) - converted to JSON_VALUE(JSON_POINTER_EXTRACT_TEXT())"),
        ]
        
        for pattern, description in unsupported_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                warnings.append(f"Converted PostgreSQL feature: {description}")
        
        return warnings 

    def detect_scalar_subqueries_in_functions(self, sql: str) -> List[str]:
        """Detect scalar subqueries inside functions that need refactoring"""
        warnings = []
        
        # Pattern 1: EXTRACT with scalar subquery
        extract_pattern = r'EXTRACT\s*\(\s*\w+\s+FROM\s*\(\s*SELECT\s+[^)]+\)\s*\)'
        if re.search(extract_pattern, sql, re.IGNORECASE):
            warnings.append("CRITICAL: Scalar subquery in EXTRACT() detected - must refactor to derived subquery")
        
        # Pattern 2: Functions with scalar subqueries
        func_patterns = [
            r'EXTRACT\s*\([^)]*\(\s*SELECT\s+[^)]*\)\s*[^)]*\)',
            r'CAST\s*\(\s*\(\s*SELECT\s+[^)]*\)\s*AS\s+[^)]*\)',
            r'COALESCE\s*\([^)]*\(\s*SELECT\s+[^)]*\)\s*[^)]*\)',
        ]
        
        for pattern in func_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                warnings.append("CRITICAL: Scalar subquery in function detected - refactor required")
        
        return warnings 