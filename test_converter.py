#!/usr/bin/env python3
"""
Simple test script for the PostgreSQL to Firebolt converter
"""

import os
from converter.query_converter import PostgreSQLToFireboltConverter

def test_basic_conversion():
    """Test basic conversion functionality"""
    print("🧪 Testing PostgreSQL to Firebolt Converter")
    print("=" * 50)
    
    # Initialize converter
    converter = PostgreSQLToFireboltConverter()
    
    # Test cases
    test_cases = [
        {
            "name": "JSON Operator Conversion",
            "postgresql": """select 
object_pri_key_1 as LAF,
object_data::json->>'IMD' as IMD,
object_data::json->>'CS' as Total_Insurance,
round((((object_data::json->>'LOAN_AMOUNT')::decimal+ (object_data::json->>'CS')::decimal)*(object_data::json->>'PF')::decimal/100),2)::text as Processing_Fees,
'dops' as source,
now() at TIME ZONE 'Asia/Kolkata'as edl_job_run
from 
tb_btc_repayment_plan_obj_txn tbrpot""",
            "expected_pattern": "JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(object_data, '/IMD'))"
        },
        {
            "name": "Simple Query",
            "postgresql": "SELECT * FROM users WHERE id = 1",
            "expected_pattern": "SELECT * FROM users WHERE id = 1"
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📝 Test {i}: {test_case['name']}")
        print("-" * 30)
        
        try:
            result = converter.convert(test_case['postgresql'])
            converted_sql = result.get('converted_sql', '')
            
            print("🔍 Input:")
            print(test_case['postgresql'][:100] + "..." if len(test_case['postgresql']) > 100 else test_case['postgresql'])
            
            print("\n✅ Output:")
            print(converted_sql[:200] + "..." if len(converted_sql) > 200 else converted_sql)
            
            if test_case['expected_pattern'] in converted_sql:
                print(f"\n✅ PASS: Contains expected pattern '{test_case['expected_pattern']}'")
            else:
                print(f"\n❌ FAIL: Missing expected pattern '{test_case['expected_pattern']}'")
                
            print(f"\n📊 Method used: {result.get('method_used', 'unknown')}")
            
        except Exception as e:
            print(f"❌ ERROR: {str(e)}")

if __name__ == "__main__":
    test_basic_conversion() 