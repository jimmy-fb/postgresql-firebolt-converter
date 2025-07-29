#!/usr/bin/env python3
"""
Test the EXTRACT subquery pattern that was failing
"""

import os
from converter.query_converter import PostgreSQLToFireboltConverter

def test_extract_subquery_pattern():
    """Test the problematic EXTRACT with subquery pattern"""
    print("🧪 Testing EXTRACT Subquery Pattern Fix")
    print("=" * 60)
    
    # The problematic pattern that was failing
    postgresql_query = """SELECT DISTINCT
CD.agreementid,
DS.BRANCH_NAME,
DS.CUSTOMER_NAME,
CD.AGREEMENTNO AS LOAN_ACCOUNT_NUMBER,
DS.DATE_OF_DISB::date,
DS.disb_amount
FROM
JAYAM_CONTRACT_DETAILS CD
LEFT JOIN JAYAM_MAST_DISBURSEMENT_REPORT DS ON CD.AGREEMENTNO = DS.LOAN_ACCOUNT_NUMBER
WHERE
EXTRACT(MONTH FROM CAST(DS.DATE_OF_DISB AS DATE)) = EXTRACT(MONTH FROM (SELECT MAX(agreementdate::date) from jayam_contract_details))
AND EXTRACT(YEAR FROM CAST(DS.DATE_OF_DISB AS DATE)) = EXTRACT(YEAR FROM (SELECT MAX(agreementdate::date) from jayam_contract_details))
ORDER BY DS.DATE_OF_DISB::date ASC"""

    # Expected pattern based on user's working solution
    expected_pattern = "(SELECT MAX(agreementdate::DATE) AS max_agreement_date FROM jayam_contract_details) AS sub"
    
    print("📝 PROBLEMATIC POSTGRESQL QUERY:")
    print("-" * 40)
    print(postgresql_query)
    
    # Test our converter
    converter = PostgreSQLToFireboltConverter()
    
    try:
        result = converter.convert(postgresql_query)
        converted_sql = result.get('converted_sql', '')
        
        print("\n🤖 OUR CONVERTER'S OUTPUT:")
        print("-" * 40)
        print(converted_sql)
        
        print(f"\n📊 Method used: {result.get('method_used', 'unknown')}")
        
        # Check if it contains the key patterns for the fix
        key_patterns = [
            "max_agreement_date",  # Should create an alias for the subquery result
            ") AS sub",            # Should create a subquery alias
            "FROM sub.max_agreement_date" # Should reference the aliased column
        ]
        
        print("\n🔍 PATTERN ANALYSIS:")
        print("-" * 40)
        found_patterns = 0
        for pattern in key_patterns:
            if pattern in converted_sql:
                print(f"✅ FOUND: {pattern}")
                found_patterns += 1
            else:
                print(f"❌ MISSING: {pattern}")
        
        # Check for the problematic inline subquery pattern
        problematic_patterns = [
            "EXTRACT(MONTH FROM (SELECT",
            "EXTRACT(YEAR FROM (SELECT"
        ]
        
        print("\n🚫 CHECKING FOR PROBLEMATIC PATTERNS:")
        print("-" * 40)
        has_problems = False
        for pattern in problematic_patterns:
            if pattern in converted_sql:
                print(f"❌ STILL HAS PROBLEM: {pattern}")
                has_problems = True
            else:
                print(f"✅ FIXED: No inline subquery in EXTRACT")
                
        if found_patterns >= 2 and not has_problems:
            print("\n🎉 SUCCESS: EXTRACT subquery pattern correctly fixed!")
        elif not has_problems:
            print("\n✅ GOOD: Avoided problematic pattern, but may use different approach")
        else:
            print("\n⚠️ NEEDS WORK: Still contains problematic EXTRACT subquery patterns")
            
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")

if __name__ == "__main__":
    test_extract_subquery_pattern() 