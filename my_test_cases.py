#!/usr/bin/env python3
"""
Custom test cases for validating converter accuracy
Add your own PostgreSQL -> Firebolt query pairs here
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from comparison_test import ComparisonTester

def create_my_test_cases():
    """Add your own test cases here"""
    tester = ComparisonTester()
    
    # Example - replace with your actual conversions
    tester.add_test_case(
        name="Your Test Case 1",
        postgresql_query="""
        SELECT 
            data->>'email' as email,
            created_at::date as signup_date
        FROM users 
        WHERE data @> '{"active": true}';
        """,
        expected_firebolt="""
        SELECT 
            JSON_EXTRACT_RAW(data, '$.email') as email,
            CAST(created_at AS DATE) as signup_date
        FROM users 
        WHERE JSON_CONTAINS(data, '{"active": true}');
        """,
        notes="Your custom conversion - replace with your actual queries"
    )
    
    # Add more test cases here:
    # tester.add_test_case(
    #     name="Your Test Case 2", 
    #     postgresql_query="YOUR_POSTGRESQL_QUERY",
    #     expected_firebolt="YOUR_FIREBOLT_CONVERSION",
    #     notes="Description of what this tests"
    # )
    
    return tester

if __name__ == '__main__':
    print("🧪 Running your custom test cases against the converter...\n")
    
    tester = create_my_test_cases()
    
    if not tester.test_cases:
        print("❌ No test cases found!")
        print("📝 Add your PostgreSQL -> Firebolt conversions to the create_my_test_cases() function")
        print("💡 Example format:")
        print("""
        tester.add_test_case(
            name="My Query Test",
            postgresql_query="SELECT data->>'name' FROM users",
            expected_firebolt="SELECT JSON_EXTRACT_RAW(data, '$.name') FROM users",
            notes="JSON extraction test"
        )
        """)
    else:
        passed, failed = tester.run_tests()
        
        print(f"\n🎯 Validation Results:")
        print(f"✅ Converter got {passed} queries exactly right")
        print(f"❌ Converter needs improvement on {failed} queries")
        print(f"📈 Overall accuracy: {(passed/(passed+failed)*100):.1f}%")
        
        if failed > 0:
            print(f"\n💡 Tip: Check the differences above to improve conversion rules") 