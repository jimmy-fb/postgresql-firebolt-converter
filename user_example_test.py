#!/usr/bin/env python3
"""
Test the user's specific example to demonstrate the correct JSON conversion
"""

import os
from converter.query_converter import PostgreSQLToFireboltConverter

def test_user_example():
    """Test the user's specific PostgreSQL query example"""
    print("🧪 Testing User's JSON Conversion Example")
    print("=" * 60)
    
    # User's original PostgreSQL query
    postgresql_query = """select 
object_pri_key_1 as LAF,
object_data::json->>'IMD' as IMD,
object_data::json->>'CS' as Total_Insurance,
round((((object_data::json->>'LOAN_AMOUNT')::decimal+ (object_data::json->>'CS')::decimal)*(object_data::json->>'PF')::decimal/100),2)::text as Processing_Fees,
'dops' as source,
now() at TIME ZONE 'Asia/Kolkata'as edl_job_run
from 
tb_btc_repayment_plan_obj_txn tbrpot"""

    # User's expected working Firebolt query
    expected_firebolt = """SELECT
  object_pri_key_1 AS LAF,
  JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(object_data, '/IMD')) AS IMD,
  JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(object_data, '/CS')) AS Total_Insurance,
  ROUND((
    (
      JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(object_data, '/LOAN_AMOUNT'))::DECIMAL +
      JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(object_data, '/CS'))::DECIMAL
    ) * JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(object_data, '/PF'))::DECIMAL / 100
  ), 2)::TEXT AS Processing_Fees,
  'dops' AS source,
  NOW() AT TIME ZONE 'Asia/Kolkata' AS edl_job_run
FROM
  tb_btc_repayment_plan_obj_txn tbrpot;"""

    print("📝 ORIGINAL POSTGRESQL QUERY:")
    print("-" * 40)
    print(postgresql_query)
    
    print("\n✅ USER'S WORKING FIREBOLT QUERY:")
    print("-" * 40)
    print(expected_firebolt)
    
    # Test our converter
    converter = PostgreSQLToFireboltConverter()
    
    try:
        result = converter.convert(postgresql_query)
        converted_sql = result.get('converted_sql', '')
        
        print("\n🤖 OUR CONVERTER'S OUTPUT:")
        print("-" * 40)
        print(converted_sql)
        
        print(f"\n📊 Method used: {result.get('method_used', 'unknown')}")
        
        # Check if it contains the key patterns
        key_patterns = [
            "JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(object_data, '/IMD'))",
            "JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(object_data, '/CS'))",
            "JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(object_data, '/LOAN_AMOUNT'))"
        ]
        
        print("\n🔍 PATTERN ANALYSIS:")
        print("-" * 40)
        for pattern in key_patterns:
            if pattern in converted_sql:
                print(f"✅ FOUND: {pattern}")
            else:
                print(f"❌ MISSING: {pattern}")
                
        if all(pattern in converted_sql for pattern in key_patterns):
            print("\n🎉 SUCCESS: All JSON patterns correctly converted!")
        else:
            print("\n⚠️ PARTIAL: Some patterns may need improvement")
            
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")

if __name__ == "__main__":
    test_user_example() 