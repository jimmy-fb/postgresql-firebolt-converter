#!/usr/bin/env python3
"""
Test script that simulates the exact live testing scenario where credentials get lost
"""

import os
import asyncio
from converter.firebolt_client import FireboltClient
from converter.query_converter import PostgreSQLToFireboltConverter
from converter.live_tester import LiveQueryTester

async def test_live_scenario():
    """Test the exact scenario that happens in live testing"""
    
    print("🧪 Testing Live Scenario Credential Persistence...")
    
    # Create components exactly like Streamlit does
    print("1️⃣ Creating FireboltClient (no credentials)...")
    firebolt_client = FireboltClient()
    
    print("2️⃣ Creating query converter...")
    query_converter = PostgreSQLToFireboltConverter(openai_api_key="dummy")
    
    print("3️⃣ Creating live tester...")
    live_tester = LiveQueryTester(
        firebolt_client=firebolt_client,
        query_converter=query_converter,
        openai_api_key="dummy"
    )
    
    print("4️⃣ Connecting to Firebolt (like setup_firebolt_connection)...")
    success = await firebolt_client.connect(
        client_id="e0wMJJHiAnn4exI1GhwFlhE3VjUmIwD1",
        client_secret="3kJRpMC65X-KU3ul_b8NUX7Th7y58lTFoyADp4vNZTmJWcCdc_SkJ_4n0tphrO07",
        account="vishnu",
        database="ugrodb",
        engine="fb_clone_db_jauneet"
    )
    
    if not success:
        print("❌ Initial connection failed")
        return False
    
    print("✅ Initial connection successful")
    
    print("5️⃣ Testing multiple query executions (like live testing retries)...")
    
    for attempt in range(1, 6):  # Test 5 attempts
        print(f"   Attempt {attempt}:")
        
        # Execute a simple query
        success, result = await firebolt_client.execute_query("SELECT 1 as test")
        
        if success:
            print(f"   ✅ Query executed successfully")
        else:
            print(f"   ❌ Query failed: {result.get('error', 'Unknown error')}")
            return False
        
        # Simulate connection loss (this is what might happen during retries)
        if attempt == 3:
            print("   🔧 Simulating connection loss...")
            firebolt_client.connection = None
    
    print("6️⃣ Testing after simulated failures...")
    
    # Try one more query after the connection was lost
    success, result = await firebolt_client.execute_query("SELECT 2 as final_test")
    
    if success:
        print("✅ Final query executed successfully after connection loss!")
        print(f"   Result: {result}")
        return True
    else:
        print(f"❌ Final query failed: {result.get('error', 'Unknown error')}")
        return False

async def main():
    try:
        success = await test_live_scenario()
        if success:
            print(f"\n🎉 Live scenario test PASSED!")
        else:
            print(f"\n❌ Live scenario test FAILED!")
        return success
    except Exception as e:
        print(f"\n💥 Test failed with exception: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = asyncio.run(main())
    exit(0 if result else 1) 