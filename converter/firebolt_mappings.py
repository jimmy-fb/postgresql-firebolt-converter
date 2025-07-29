# PostgreSQL to Firebolt conversion mappings

# JSON operator mappings - Updated with correct Firebolt syntax
JSON_OPERATORS = {
    # PostgreSQL -> Firebolt
    '->': 'JSON_VALUE(JSON_POINTER_EXTRACT_TEXT',  # Will be handled specially in conversion_rules
    '->>': 'JSON_VALUE(JSON_POINTER_EXTRACT_TEXT',  # Will be handled specially  
    '#>': 'JSON_VALUE(JSON_POINTER_EXTRACT_TEXT',   # Will be handled specially
    '#>>': 'JSON_VALUE(JSON_POINTER_EXTRACT_TEXT',  # Will be handled specially
    '@>': 'JSON_CONTAINS',
    '<@': 'JSON_CONTAINS',  # Reverse contains
    '?': 'JSON_EXISTS',
    '?|': 'JSON_EXISTS_ANY',
    '?&': 'JSON_EXISTS_ALL'
}

# Function mappings - Updated with correct Firebolt functions
FUNCTION_MAPPINGS = {
    # Date/Time functions
    'now()': 'NOW()',  # Keep as NOW() - this is correct for Firebolt
    'current_timestamp': 'NOW()', 
    'current_date': 'CURRENT_DATE',
    'current_time': 'CURRENT_TIME',
    'date_trunc': 'DATE_TRUNC',
    'extract': 'EXTRACT',
    'date_part': 'EXTRACT',
    
    # String functions
    'length': 'LENGTH',
    'char_length': 'LENGTH', 
    'character_length': 'LENGTH',
    'position': 'STRPOS',
    'substring': 'SUBSTR',
    'substr': 'SUBSTR',
    'left': 'SUBSTR',  # Will need special handling
    'right': 'SUBSTR', # Will need special handling
    'split_part': 'SPLIT',
    'regexp_replace': 'REGEXP_REPLACE',
    'regexp_match': 'REGEXP_EXTRACT',
    'concat': 'CONCAT',
    'trim': 'TRIM',
    'ltrim': 'LTRIM',
    'rtrim': 'RTRIM',
    'upper': 'UPPER',
    'lower': 'LOWER',
    'replace': 'REPLACE',
    
    # JSON functions - Updated
    'json_extract': 'JSON_VALUE(JSON_POINTER_EXTRACT_TEXT',
    'json_extract_path': 'JSON_VALUE(JSON_POINTER_EXTRACT_TEXT',
    'json_extract_path_text': 'JSON_VALUE(JSON_POINTER_EXTRACT_TEXT',
    
    # Math functions
    'abs': 'ABS',
    'ceil': 'CEILING',
    'ceiling': 'CEILING',
    'floor': 'FLOOR',
    'round': 'ROUND',
    'power': 'POW',
    'sqrt': 'SQRT',
    'ln': 'LN',
    'log': 'LOG',
    'exp': 'EXP',
    'sin': 'SIN',
    'cos': 'COS',
    'tan': 'TAN',
    'random': 'RANDOM',
    
    # Aggregate functions
    'array_agg': 'ARRAY_AGG',
    'string_agg': 'ARRAY_TO_STRING(ARRAY_AGG',  # Special handling needed
    'count': 'COUNT',
    'sum': 'SUM',
    'avg': 'AVG',
    'min': 'MIN',
    'max': 'MAX',
    'stddev': 'STDDEV',
    'variance': 'VARIANCE',
    
    # Array functions
    'array_length': 'ARRAY_LENGTH',
    'array_position': 'ARRAY_POSITION',
    'array_append': 'ARRAY_APPEND',
    'array_prepend': 'ARRAY_PREPEND',
    'array_cat': 'ARRAY_CONCAT',
    'array_remove': 'ARRAY_REMOVE',
    'unnest': 'UNNEST',
    
    # Window functions
    'row_number': 'ROW_NUMBER',
    'rank': 'RANK', 
    'dense_rank': 'DENSE_RANK',
    'lag': 'LAG',
    'lead': 'LEAD',
    'first_value': 'FIRST_VALUE',
    'last_value': 'LAST_VALUE',
    'nth_value': 'NTH_VALUE'
}

# Data type mappings - Updated to prefer :: casting
DATA_TYPE_MAPPINGS = {
    # PostgreSQL -> Firebolt
    'varchar': 'TEXT',
    'character varying': 'TEXT', 
    'char': 'TEXT',
    'character': 'TEXT',
    'text': 'TEXT',
    'integer': 'INT',
    'int': 'INT',
    'int4': 'INT',
    'bigint': 'BIGINT',
    'int8': 'BIGINT',
    'smallint': 'INT',
    'int2': 'INT',
    'decimal': 'DECIMAL',
    'numeric': 'DECIMAL',
    'real': 'REAL',
    'float4': 'REAL',
    'double precision': 'DOUBLE',
    'float8': 'DOUBLE',
    'float': 'DOUBLE',
    'boolean': 'BOOLEAN',
    'bool': 'BOOLEAN',
    'date': 'DATE',
    'timestamp': 'TIMESTAMP',
    'timestamptz': 'TIMESTAMPTZ',
    'timestamp with time zone': 'TIMESTAMPTZ',
    'timestamp without time zone': 'TIMESTAMP',
    'time': 'TIME',
    'timetz': 'TIME',
    'interval': 'INTERVAL',
    'json': 'TEXT',  # Firebolt stores JSON as TEXT
    'jsonb': 'TEXT', # Firebolt stores JSON as TEXT
    'uuid': 'TEXT',
    'bytea': 'BYTEA',
    'array': 'ARRAY'
}

# PostgreSQL-specific features that need warnings or conversion
UNSUPPORTED_FEATURES = {
    'RETURNING': 'RETURNING clause is not supported in Firebolt',
    'WITH RECURSIVE': 'Recursive CTEs have limited support in Firebolt',
    'LATERAL': 'LATERAL joins may not be supported in Firebolt', 
    'TABLESAMPLE': 'TABLESAMPLE may need alternative approach in Firebolt',
    'UNNEST': 'UNNEST function may need conversion to table-valued function',
    'GENERATE_SERIES': 'GENERATE_SERIES may need alternative implementation',
    'CROSSTAB': 'CROSSTAB function not available in Firebolt',
    'CURSOR': 'Cursors are not supported in Firebolt',
    'LISTEN': 'LISTEN/NOTIFY not supported in Firebolt',
    'NOTIFY': 'LISTEN/NOTIFY not supported in Firebolt',
    'CREATE TRIGGER': 'Triggers are not supported in Firebolt',
    'CREATE FUNCTION': 'User-defined functions may need conversion to UDFs',
    'VACUUM': 'VACUUM is not needed in Firebolt (automatic optimization)',
    'ANALYZE': 'ANALYZE is not needed in Firebolt (automatic statistics)',
    'FILTER': 'FILTER clause converted to CASE WHEN syntax for Firebolt compatibility',
    
    # New learnings from user feedback
    'SCALAR_SUBQUERY_IN_EXTRACT': 'Scalar subqueries inside EXTRACT() must be refactored to derived subqueries',
    'JSON_CASTING': 'JSON casting (::json) not supported - JSON stored as TEXT in Firebolt',
    'REDSHIFT_JSON_SYNTAX': 'Redshift/PostgreSQL JSON syntax (->, ->>) converted to Firebolt JSON functions',
}

# Common conversion patterns and their Firebolt equivalents
CONVERSION_GUIDELINES = {
    'FILTER_CLAUSE': {
        'postgresql': 'SUM(amount) FILTER (WHERE status = \'active\')',
        'firebolt': 'SUM(CASE WHEN status = \'active\' THEN amount ELSE 0 END)',
        'note': 'FILTER clauses must be converted to CASE WHEN expressions'
    },
    'JSON_ACCESS': {
        'postgresql': 'column->>\'key\'',
        'firebolt': 'JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(column, \'/key\'))',
        'note': 'Always use JSON_VALUE with JSON_POINTER_EXTRACT_TEXT for JSON field access'
    },
    'SCALAR_SUBQUERY_EXTRACT': {
        'postgresql': 'EXTRACT(YEAR FROM (SELECT MAX(date) FROM table))',
        'firebolt': 'EXTRACT(YEAR FROM max_date) FROM (SELECT MAX(date) AS max_date FROM table) AS sub',
        'note': 'Refactor scalar subqueries in functions to derived subqueries'
    },
    'COMPLEX_EXTRACT_SUBQUERY': {
        'postgresql': '''WHERE 
        EXTRACT(MONTH FROM CAST(DS.DATE_OF_DISB AS DATE)) = EXTRACT(MONTH FROM (SELECT MAX_BUSINESS_DATE()))
        AND EXTRACT(YEAR FROM CAST(DS.DATE_OF_DISB AS DATE)) = EXTRACT(YEAR FROM (SELECT MAX_BUSINESS_DATE()))''',
        'firebolt': '''FROM table_name,
        (SELECT MAX(business_date) AS max_business_date FROM business_table) AS sub
        WHERE 
        EXTRACT(MONTH FROM CAST(DS.DATE_OF_DISB AS DATE)) = EXTRACT(MONTH FROM sub.max_business_date)
        AND EXTRACT(YEAR FROM CAST(DS.DATE_OF_DISB AS DATE)) = EXTRACT(YEAR FROM sub.max_business_date)''',
        'note': 'Move scalar subqueries to FROM clause as derived tables'
    },
    'JSON_STORAGE': {
        'postgresql': 'column::json',
        'firebolt': 'column /* JSON stored as TEXT */',
        'note': 'Firebolt stores JSON as TEXT, no need for ::json casting'
    }
}

# Operators that need conversion
OPERATOR_MAPPINGS = {
    '||': 'CONCAT',  # String concatenation
    '~~': 'LIKE',    # LIKE operator
    '~~*': 'ILIKE',  # Case-insensitive LIKE
    '!~~': 'NOT LIKE',
    '!~~*': 'NOT ILIKE',
    '~': 'REGEXP_LIKE',  # Regular expression match
    '~*': 'REGEXP_LIKE',  # Case-insensitive regex
    '!~': 'NOT REGEXP_LIKE',
    '!~*': 'NOT REGEXP_LIKE',
}

# Common table expressions that need attention
CTE_PATTERNS = {
    'RECURSIVE': 'Limited support - may need rewriting',
    'MATERIALIZED': 'Not supported - remove MATERIALIZED keyword',
    'NOT MATERIALIZED': 'Not needed in Firebolt',
}

# Window function frame specifications that may differ
WINDOW_FRAME_MAPPINGS = {
    'ROWS UNBOUNDED PRECEDING': 'ROWS UNBOUNDED PRECEDING',
    'ROWS CURRENT ROW': 'ROWS CURRENT ROW', 
    'RANGE UNBOUNDED PRECEDING': 'RANGE UNBOUNDED PRECEDING',
    'RANGE CURRENT ROW': 'RANGE CURRENT ROW',
    # Add more as needed based on Firebolt documentation
} 