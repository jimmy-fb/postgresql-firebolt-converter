#!/usr/bin/env python3
"""
PostgreSQL to Firebolt Query Converter - Comparison Test
Allows testing converter output against known correct Firebolt syntax
"""

import os
import sys
import json
from dotenv import load_dotenv
from difflib import unified_diff
from typing import Dict, List

# Load environment variables
load_dotenv()

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from converter.query_converter import PostgreSQLToFireboltConverter

class ComparisonTester:
    def __init__(self):
        self.converter = PostgreSQLToFireboltConverter()
        self.test_cases = []
    
    def add_test_case(self, name: str, postgresql_query: str, expected_firebolt: str, notes: str = ""):
        """Add a test case for comparison"""
        self.test_cases.append({
            'name': name,
            'postgresql_query': postgresql_query.strip(),
            'expected_firebolt': expected_firebolt.strip(),
            'notes': notes
        })
    
    def normalize_sql(self, query: str) -> str:
        """Normalize SQL for comparison (remove extra whitespace, etc.)"""
        import re
        # Remove extra whitespace and normalize
        query = re.sub(r'\s+', ' ', query.strip())
        # Remove trailing semicolon for comparison
        query = query.rstrip(';')
        return query
    
    def compare_queries(self, converted: str, expected: str) -> Dict:
        """Compare converted query with expected result"""
        converted_norm = self.normalize_sql(converted)
        expected_norm = self.normalize_sql(expected)
        
        exact_match = converted_norm == expected_norm
        
        # Calculate similarity (simple approach)
        if not exact_match:
            # Show differences
            diff = list(unified_diff(
                expected_norm.splitlines(keepends=True),
                converted_norm.splitlines(keepends=True),
                fromfile='Expected',
                tofile='Converted',
                lineterm=''
            ))
        else:
            diff = []
        
        return {
            'exact_match': exact_match,
            'converted_normalized': converted_norm,
            'expected_normalized': expected_norm,
            'diff': diff
        }
    
    def run_tests(self):
        """Run all comparison tests"""
        print("🔍 PostgreSQL to Firebolt Converter - Comparison Tests\n")
        print("=" * 80)
        
        passed = 0
        failed = 0
        
        for i, test_case in enumerate(self.test_cases, 1):
            print(f"\n📋 Test {i}: {test_case['name']}")
            print("-" * 60)
            
            if test_case['notes']:
                print(f"📝 Notes: {test_case['notes']}")
            
            print("🔵 PostgreSQL Query:")
            print(test_case['postgresql_query'])
            
            print("\n🎯 Expected Firebolt:")
            print(test_case['expected_firebolt'])
            
            # Convert using our tool
            result = self.converter.convert(test_case['postgresql_query'])
            converted = result.get('converted_sql', '')
            
            print("\n🔄 Converter Output:")
            print(converted)
            
            # Compare results
            comparison = self.compare_queries(converted, test_case['expected_firebolt'])
            
            if comparison['exact_match']:
                print("\n✅ PASSED - Exact match!")
                passed += 1
            else:
                print("\n❌ FAILED - Output differs from expected")
                print("\n📊 Differences:")
                for line in comparison['diff']:
                    print(line.rstrip())
                failed += 1
            
            if result['warnings']:
                print("\n⚠️ Warnings:")
                for warning in result['warnings']:
                    print(f"  • {warning}")
            
            if result['explanations']:
                print("\n✅ Conversions Applied:")
                for explanation in result['explanations']:
                    print(f"  • {explanation}")
            
            print("\n" + "=" * 80)
        
        # Summary
        print(f"\n📊 Test Summary:")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"📈 Success Rate: {(passed/(passed+failed)*100):.1f}%")
        
        return passed, failed

def create_sample_tests():
    """Create sample test cases - you can modify these or add your own"""
    tester = ComparisonTester()
    
    # JSON Operations Tests
    tester.add_test_case(
        name="JSON Arrow Operator",
        postgresql_query="SELECT data->'name' FROM users;",
        expected_firebolt="SELECT JSON_EXTRACT(data, '$.name') FROM users;",
        notes="Basic JSON field extraction"
    )
    
    tester.add_test_case(
        name="JSON Double Arrow Operator", 
        postgresql_query="SELECT data->>'email' FROM users;",
        expected_firebolt="SELECT JSON_EXTRACT_RAW(data, '$.email') FROM users;",
        notes="JSON field extraction as text"
    )
    
    # Type Casting Tests
    tester.add_test_case(
        name="Type Casting with ::",
        postgresql_query="SELECT id::text, amount::decimal FROM orders;",
        expected_firebolt="SELECT CAST(id AS TEXT), CAST(amount AS DECIMAL) FROM orders;",
        notes="PostgreSQL :: operator to CAST function"
    )
    
    # Date Functions
    tester.add_test_case(
        name="NOW() Function",
        postgresql_query="SELECT now() as current_time;",
        expected_firebolt="SELECT CURRENT_TIMESTAMP as current_time;",
        notes="NOW() to CURRENT_TIMESTAMP conversion"
    )
    
    # String Functions
    tester.add_test_case(
        name="String Concatenation",
        postgresql_query="SELECT first_name || ' ' || last_name as full_name FROM users;",
        expected_firebolt="SELECT CONCAT(CONCAT(first_name, ' '), last_name) as full_name FROM users;",
        notes="|| operator to CONCAT function"
    )
    
    return tester

def interactive_test():
    """Interactive mode to add custom test cases"""
    tester = ComparisonTester()
    
    print("🔍 Interactive Comparison Test Mode")
    print("Enter your test cases (press Enter twice to finish)\n")
    
    while True:
        print("\n" + "-" * 40)
        name = input("Test name (or 'quit' to exit): ").strip()
        if name.lower() == 'quit':
            break
        
        print("\nEnter PostgreSQL query:")
        postgresql_query = input().strip()
        
        print("\nEnter expected Firebolt result:")
        expected_firebolt = input().strip()
        
        notes = input("\nOptional notes: ").strip()
        
        tester.add_test_case(name, postgresql_query, expected_firebolt, notes)
        print("✅ Test case added!")
    
    if tester.test_cases:
        print(f"\n🚀 Running {len(tester.test_cases)} test case(s)...")
        tester.run_tests()
    else:
        print("No test cases to run.")

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--interactive':
        interactive_test()
    elif len(sys.argv) > 1 and sys.argv[1] == '--custom':
        # You can add your own test cases here
        print("Add your custom test cases in the create_custom_tests() function")
    else:
        # Run sample tests
        print("Running sample test cases...")
        print("Use --interactive for interactive mode")
        print("Use --custom to run your custom test cases\n")
        
        tester = create_sample_tests()
        tester.run_tests()
        
        print("\n💡 Tips:")
        print("  • Modify the test cases in create_sample_tests() function")
        print("  • Run with --interactive to add custom test cases")
        print("  • Check failed tests to improve conversion rules") 