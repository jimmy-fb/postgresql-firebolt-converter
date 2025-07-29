#!/usr/bin/env python3
"""
Test script for PostgreSQL to Firebolt Query Converter
Demonstrates the conversion functionality
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from converter.query_converter import PostgreSQLToFireboltConverter

def test_converter():
    """Test the converter with various PostgreSQL queries"""
    
    converter = PostgreSQLToFireboltConverter()
    
    test_queries = [
        # JSON Operations
        {
            "name": "JSON Arrow Operators",
            "query": """
            SELECT 
                user_id,
                data->>'name' as user_name,
                data->'address'->>'city' as city,
                data @> '{"active": true}' as is_active
            FROM users 
            WHERE data->>'status' = 'active';
            """
        },
        
        # Data Type Casting
        {
            "name": "Type Casting",
            "query": """
            SELECT 
                id::text as user_id,
                created_at::date as signup_date,
                amount::decimal(10,2) as formatted_amount,
                CAST(score AS integer) as int_score
            FROM transactions;
            """
        },
        
        # Date Functions
        {
            "name": "Date Functions",
            "query": """
            SELECT 
                now() as current_time,
                date_trunc('day', created_at) as day_created,
                EXTRACT(EPOCH FROM created_at) as timestamp,
                age(created_at, now()) as account_age
            FROM events 
            WHERE created_at >= now() - interval '30 days';
            """
        },
        
        # String Functions and Operators
        {
            "name": "String Operations",
            "query": """
            SELECT 
                first_name || ' ' || last_name as full_name,
                position('test' in description) as test_position,
                substring(email from 1 for 10) as email_prefix,
                length(description) as desc_length
            FROM users;
            """
        },
        
        # Array and Window Functions
        {
            "name": "Array and Window Functions",
            "query": """
            SELECT 
                array_agg(distinct category) as categories,
                row_number() OVER (ORDER BY created_at) as row_num,
                rank() OVER (PARTITION BY category ORDER BY score DESC) as category_rank
            FROM products 
            GROUP BY category;
            """
        },
        
        # Complex Query with Multiple Features
        {
            "name": "Complex Multi-Feature Query",
            "query": """
            WITH user_stats AS (
                SELECT 
                    user_id,
                    data->>'email' as email,
                    jsonb_extract_path_text(preferences, 'notifications') as notification_pref,
                    created_at::date as signup_date,
                    COUNT(*) OVER (PARTITION BY data->>'country') as country_users
                FROM users 
                WHERE data @> '{"active": true}'
                AND created_at >= now() - interval '1 year'
            )
            SELECT 
                email,
                notification_pref,
                signup_date,
                country_users,
                age(signup_date, current_date) as account_duration
            FROM user_stats
            ORDER BY signup_date DESC
            LIMIT 100;
            """
        }
    ]
    
    print("🔄 PostgreSQL to Firebolt Query Converter Test\n")
    print("=" * 80)
    
    for i, test in enumerate(test_queries, 1):
        print(f"\n📋 Test {i}: {test['name']}")
        print("-" * 60)
        
        print("🔵 PostgreSQL Query:")
        print(test['query'].strip())
        
        # Convert the query
        result = converter.convert(test['query'].strip())
        
        print("\n🟢 Firebolt Query:")
        print(result['converted_query'])
        
        if result['warnings']:
            print("\n⚠️  Warnings:")
            for warning in result['warnings']:
                print(f"  • {warning}")
        
        if result['explanations']:
            print("\n✅ Conversions Applied:")
            for explanation in result['explanations']:
                print(f"  • {explanation}")
        
        print("\n" + "=" * 80)
    
    print("\n🎉 Test completed! Check the conversions above.")
    print("💡 Tip: Set OPENAI_API_KEY in your environment for AI-powered improvements.")

if __name__ == '__main__':
    test_converter() 